"""Detect claim-payment anomalies and write structured incident evidence as JSON.

Usage:
  python scripts/detect_anomalies.py --demo-date 2026-03-01 [--schedule config/batch_schedule.json]
      [--db FILE] [--source-db FILE] [--label NAME] [--output FILE]

Reuses reconcile_duckdb.reconcile(), which reads the SQLite source (read-only),
the DuckDB etl_batch_audit table (staging counts for the latest load run), and
fact_claim_payment (read-only).

Checks (each finding has check, category, batch_id, expected, observed, severity):

  zero_row_load          ETL_INTEGRITY  CRITICAL  source batch has rows, target has 0
  row_count_mismatch     ETL_INTEGRITY  HIGH      source rows != staging or target rows
  amount_mismatch        ETL_INTEGRITY  HIGH      source paise != staging or target paise
  missing_expected_batch COMPLETENESS   HIGH      batch due by the demo date per the schedule
                                                  is absent from the source, or never loaded
  stale_data             FRESHNESS      MEDIUM (1 day late) / HIGH (2+ days late)
                                                  latest loaded batch is older than expected
  unusual_payment_total  BUSINESS_ANOMALY MEDIUM / HIGH (total >= 2x baseline median)
                                                  batch reconciles source = target, but its
                                                  total is far from prior clean batches

Unusual payment total rule:
  * Only batches that reconcile cleanly (source = staging = target) are evaluated,
    so an ETL defect is never mislabelled as a business anomaly.
  * Baseline: up to BASELINE_WINDOW (28) most recent PRIOR clean batches that were
    not themselves flagged. At least MIN_HISTORY (14) are required; batches with
    less history are reported as not evaluated, not as healthy.
  * Flag when BOTH hold:
      robust z = |total - median| / (1.4826 * MAD) >= ROBUST_Z_THRESHOLD (6.0)
      |total / median - 1|                         >= MIN_RELATIVE_DEVIATION (0.50)
    On the clean 60-batch dataset the worst values are z 4.48 and 35%.

Exit codes: 0 = no findings, 2 = findings written, 1 = error.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import statistics
import sys
from datetime import date, datetime, timedelta
from pathlib import Path

from claims_workdir import (
    REPO_ROOT,
    duckdb_path,
    ensure_outside_repo,
    get_work_dir,
    incidents_dir,
)
from reconcile_duckdb import evaluate, reconcile

SPIKE_RULE = {
    "baseline": "prior batches that reconciled cleanly and were not flagged",
    "baseline_window": 28,
    "min_history": 14,
    "robust_z_threshold": 6.0,
    "min_relative_deviation": 0.50,
    "high_severity_ratio": 2.0,
}
DEFAULT_SCHEDULE = REPO_ROOT / "config" / "batch_schedule.json"
SEVERITY_ORDER = ["CRITICAL", "HIGH", "MEDIUM", "LOW"]


def load_schedule(path: Path) -> dict:
    s = json.loads(path.read_text(encoding="utf-8"))
    s["first_batch_date"] = date.fromisoformat(s["first_batch_date"])
    return s


def expected_batches(schedule: dict, demo_date: date) -> list[tuple[str, date]]:
    last_due = demo_date - timedelta(days=schedule["arrival_lag_days"])
    out, d = [], schedule["first_batch_date"]
    while d <= last_due:
        out.append((d.strftime(schedule["batch_id_format"]), d))
        d += timedelta(days=schedule["frequency_days"])
    return out


def finding(check, category, batch_id, expected, observed, unit, severity, message, **evidence) -> dict:
    return {"check": check, "category": category, "batch_id": batch_id, "expected": expected,
            "observed": observed, "unit": unit, "severity": severity, "message": message,
            "evidence": evidence}


def etl_findings(per_batch: list[dict]) -> list[dict]:
    out = []
    for p in per_batch:
        b = p["batch_id"]
        ev = {k: p[k] for k in ("source_rows", "staging_rows", "target_rows", "source_amount_paise",
                                "staging_amount_paise", "target_amount_paise")}
        if p["staging_rows"] is None:
            out.append(finding("missing_expected_batch", "COMPLETENESS", b, "loaded", "not loaded",
                               "status", "HIGH", f"{b} exists in the source but the latest load "
                               "run has no audit record for it", **ev))
            continue
        if p["source_rows"] > 0 and p["target_rows"] == 0:
            out.append(finding("zero_row_load", "ETL_INTEGRITY", b, p["source_rows"], 0, "rows",
                               "CRITICAL", f"{b} has {p['source_rows']:,} source payments but 0 "
                               "were loaded to fact_claim_payment", **ev))
            continue  # count/amount gaps are implied; do not raise duplicate alerts
        if not p["source_rows"] == p["staging_rows"] == p["target_rows"]:
            out.append(finding("row_count_mismatch", "ETL_INTEGRITY", b, p["source_rows"],
                               p["target_rows"], "rows", "HIGH",
                               f"{b}: source {p['source_rows']:,}, staging {p['staging_rows']:,}, "
                               f"target {p['target_rows']:,} rows", **ev,
                               row_diff=p["target_rows"] - p["source_rows"]))
        if not p["source_amount_paise"] == p["staging_amount_paise"] == p["target_amount_paise"]:
            out.append(finding("amount_mismatch", "ETL_INTEGRITY", b, p["source_amount_paise"],
                               p["target_amount_paise"], "paise", "HIGH",
                               f"{b}: target total differs from source by "
                               f"{p['target_amount_paise'] - p['source_amount_paise']:,} paise", **ev,
                               amount_diff_paise=p["target_amount_paise"] - p["source_amount_paise"]))
    return out


def schedule_findings(schedule: dict, demo_date: date, per_batch: list[dict]) -> list[dict]:
    out = []
    by_id = {p["batch_id"]: p for p in per_batch}
    due = expected_batches(schedule, demo_date)
    for batch_id, d in due:
        if batch_id not in by_id:
            out.append(finding("missing_expected_batch", "COMPLETENESS", batch_id,
                               schedule["expected_rows_per_batch"], 0, "rows", "HIGH",
                               f"{batch_id} was due on {d} but is absent from the source",
                               due_date=d.isoformat(), demo_date=demo_date.isoformat()))
    if not due:
        return out
    loaded = [datetime.strptime(p["batch_id"], schedule["batch_id_format"]).date()
              for p in per_batch if p["target_rows"]]
    latest = max(loaded) if loaded else None
    expected_latest_id, expected_latest = due[-1]
    lag = (expected_latest - latest).days if latest else None
    if latest is None or lag > schedule["freshness_grace_days"]:
        severity = "MEDIUM" if lag == 1 else "HIGH"
        out.append(finding("stale_data", "FRESHNESS", expected_latest_id, expected_latest.isoformat(),
                           latest.isoformat() if latest else None, "latest_batch_date", severity,
                           f"latest loaded batch is {latest} but {expected_latest} was expected "
                           f"by demo date {demo_date} ({lag} day(s) late)",
                           demo_date=demo_date.isoformat(), days_late=lag,
                           grace_days=schedule["freshness_grace_days"]))
    return out


def spike_findings(per_batch: list[dict], schedule: dict, demo_date: date) -> tuple[list[dict], dict]:
    out, baseline, not_evaluated = [], [], []
    rule = SPIKE_RULE
    for p in per_batch:  # chronological
        d = datetime.strptime(p["batch_id"], schedule["batch_id_format"]).date()
        if d > demo_date or p["status"] != "MATCH":
            continue
        total = p["target_amount_paise"]
        history = baseline[-rule["baseline_window"]:]
        if len(history) < rule["min_history"]:
            not_evaluated.append(p["batch_id"])
            baseline.append(total)
            continue
        median = statistics.median(history)
        mad = statistics.median(abs(x - median) for x in history)
        robust_z = abs(total - median) / (1.4826 * mad) if mad else float("inf")
        rel_dev = total / median - 1
        if robust_z >= rule["robust_z_threshold"] and abs(rel_dev) >= rule["min_relative_deviation"]:
            ratio = total / median
            severity = "HIGH" if ratio >= rule["high_severity_ratio"] or ratio <= 1 / rule["high_severity_ratio"] else "MEDIUM"
            out.append(finding(
                "unusual_payment_total", "BUSINESS_ANOMALY", p["batch_id"], int(median), total, "paise",
                severity, f"{p['batch_id']} paid {total:,} paise, {ratio:.2f}x the baseline median; "
                "source and target agree, so this is a business anomaly, not an ETL mismatch",
                direction="spike" if rel_dev > 0 else "drop", baseline_batches=len(history),
                baseline_median_paise=int(median), baseline_mad_paise=int(mad),
                robust_z=round(robust_z, 2), relative_deviation=round(rel_dev, 4),
                source_amount_paise=p["source_amount_paise"], target_amount_paise=total,
                reconciled=True))
            continue  # flagged batches never enter the baseline
        baseline.append(total)
    return out, {"not_evaluated_insufficient_history": not_evaluated}


def detect(work_dir: Path, db: Path, demo_date: date, schedule_path: Path,
           source_db: Path | None = None, label: str | None = None) -> dict:
    schedule = load_schedule(schedule_path)
    rec = reconcile(work_dir, db, source_db)
    rec_checks = evaluate(rec, None)
    per_batch = rec["per_batch"]

    findings = etl_findings(per_batch)
    findings += schedule_findings(schedule, demo_date, per_batch)
    spikes, spike_info = spike_findings(per_batch, schedule, demo_date)
    findings += spikes
    findings.sort(key=lambda f: (SEVERITY_ORDER.index(f["severity"]), f["batch_id"] or "", f["check"]))
    for i, f in enumerate(findings, start=1):
        f["finding_id"] = f"F{i:03d}"

    digest = hashlib.sha256(json.dumps(
        [(f["check"], f["batch_id"], f["expected"], f["observed"]) for f in findings]).encode()).hexdigest()
    by_sev = {s: sum(f["severity"] == s for f in findings) for s in SEVERITY_ORDER}
    by_check: dict[str, int] = {}
    for f in findings:
        by_check[f["check"]] = by_check.get(f["check"], 0) + 1

    return {
        "incident_id": f"INC-{demo_date:%Y%m%d}-{digest[:8].upper()}" if findings else None,
        "label": label,
        "status": "OPEN" if findings else "NO_INCIDENT",
        "highest_severity": next((s for s in SEVERITY_ORDER if by_sev[s]), None),
        "demo_date": demo_date.isoformat(),
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "sources": {"sqlite": rec["source_db"], "duckdb": rec["duckdb"],
                    "latest_load_run": rec["latest_load_run"]},
        "schedule": {**schedule, "first_batch_date": schedule["first_batch_date"].isoformat(),
                     "file": str(schedule_path)},
        "spike_rule": {**SPIKE_RULE, **spike_info},
        "reconciliation": {
            "passed": all(rec_checks.values()),
            "failed_checks": [k for k, v in rec_checks.items() if not v],
            "totals": rec["totals"],
            "duplicates": rec["duplicates"],
            "differences": rec["differences"],
        },
        "summary": {"findings": len(findings), "by_severity": by_sev, "by_check": by_check},
        "findings": findings,
    }


def print_incident(inc: dict) -> None:
    rec = inc["reconciliation"]
    print(f"Label / demo date : {inc['label'] or '-'} / {inc['demo_date']}")
    print(f"DuckDB            : {inc['sources']['duckdb']}")
    print(f"Reconciliation    : {'PASSED' if rec['passed'] else 'FAILED'} "
          f"(target {rec['totals']['target_rows']:,} rows, {rec['totals']['target_amount_paise']:,} paise)")
    print(f"Status            : {inc['status']}" + (f" {inc['incident_id']} highest={inc['highest_severity']}"
                                                    if inc["incident_id"] else ""))
    for f in inc["findings"]:
        print(f"  {f['finding_id']} [{f['severity']}] {f['check']} {f['batch_id']}: "
              f"expected {f['expected']} observed {f['observed']} {f['unit']}")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--demo-date", required=True, type=date.fromisoformat, help="as-of date, YYYY-MM-DD")
    parser.add_argument("--schedule", type=Path, default=DEFAULT_SCHEDULE)
    parser.add_argument("--db", type=Path, help="DuckDB file (default: main warehouse)")
    parser.add_argument("--source-db", type=Path, help="alternative SQLite source")
    parser.add_argument("--label", help="scenario name stored in the incident")
    parser.add_argument("--output", type=Path, help="incident JSON path (default: $CLAIMS_WORK_DIR\\incidents\\...)")
    args = parser.parse_args()

    work_dir = get_work_dir(create=False)
    db = ensure_outside_repo(args.db, "--db") if args.db else duckdb_path(work_dir)
    inc = detect(work_dir, db, args.demo_date, args.schedule, args.source_db, args.label)
    print_incident(inc)
    name = f"{args.label or 'incident'}_{args.demo_date:%Y%m%d}.json"
    out = ensure_outside_repo(args.output or incidents_dir(work_dir) / name, "--output")
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(inc, indent=2) + "\n", encoding="utf-8")
    print(f"Incident JSON     : {out}")
    sys.exit(2 if inc["findings"] else 0)


if __name__ == "__main__":
    main()
