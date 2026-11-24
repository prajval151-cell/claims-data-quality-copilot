"""Verify Milestone 3 anomaly detection against a healthy run and simulated failures.

Every scenario's findings must EXACTLY equal its expected set of (check, batch_id)
pairs: a missing alert or an extra (unexpected) alert fails the scenario.

Scenarios (simulation files live in $CLAIMS_WORK_DIR\\simulations\\milestone3 and are
rebuilt on each run; the main SQLite source and main DuckDB warehouse are only read):

  healthy          main warehouse, demo date 2026-03-01           -> no findings
  zero_row_load    staging drops all of BATCH-20260115            -> zero_row_load
  partial_load     staging loses 25 rows of BATCH-20260210        -> row_count_mismatch + amount_mismatch
  amount_drift     staging adds 100 paise to one BATCH-20260205 row -> amount_mismatch
  missing_batches  main warehouse, demo date 2026-03-03           -> missing_expected_batch x2 + stale_data
  payment_spike    SQLite copy with BATCH-20260220 amounts x3,
                   loaded cleanly (source = target)               -> unusual_payment_total only,
                                                                     reconciliation must pass

Incident JSON per scenario and a validation summary are written to
$CLAIMS_WORK_DIR\\incidents\\milestone3.
"""

from __future__ import annotations

import json
import sqlite3
import sys
from datetime import date
from pathlib import Path

from claims_workdir import (
    db_path,
    duckdb_path,
    get_work_dir,
    incidents_dir,
    simulations_dir,
)
from detect_anomalies import DEFAULT_SCHEDULE, detect
from load_duckdb import run_load

SPIKE_BATCH = "BATCH-20260220"
SPIKE_MULTIPLIER = 3


def fresh(path: Path) -> Path:
    """Remove a previous simulation file (only ever under simulations/milestone3)."""
    for p in (path, path.with_name(path.name + ".wal")):
        p.unlink(missing_ok=True)
    return path


def make_spike_source(work_dir: Path, target: Path) -> Path:
    """Copy the read-only Milestone 1 SQLite source and multiply one batch's amounts."""
    fresh(target)
    src = sqlite3.connect(f"{db_path(work_dir).as_uri()}?mode=ro", uri=True)
    dst = sqlite3.connect(target)
    src.backup(dst)
    src.close()
    with dst:
        dst.execute("UPDATE payments SET amount_paise = amount_paise * ? WHERE batch_id = ?",
                    [SPIKE_MULTIPLIER, SPIKE_BATCH])
    dst.close()
    return target


def main() -> None:
    work_dir = get_work_dir(create=False)
    sim = simulations_dir(work_dir) / "milestone3"
    out_dir = incidents_dir(work_dir) / "milestone3"
    sim.mkdir(parents=True, exist_ok=True)
    out_dir.mkdir(parents=True, exist_ok=True)
    main_db = duckdb_path(work_dir)

    scenarios = [
        {"name": "healthy", "db": main_db, "demo": date(2026, 3, 1), "expected": set()},
        {"name": "zero_row_load", "demo": date(2026, 3, 1),
         "load": {"drop_batch": "BATCH-20260115"},
         "expected": {("zero_row_load", "BATCH-20260115")}},
        {"name": "partial_load", "demo": date(2026, 3, 1),
         "load": {"drop_rows": ("BATCH-20260210", 25)},
         "expected": {("row_count_mismatch", "BATCH-20260210"), ("amount_mismatch", "BATCH-20260210")}},
        {"name": "amount_drift", "demo": date(2026, 3, 1),
         "load": {"amount_delta": ("BATCH-20260205", 100)},
         "expected": {("amount_mismatch", "BATCH-20260205")}},
        {"name": "missing_batches", "db": main_db, "demo": date(2026, 3, 3),
         "expected": {("missing_expected_batch", "BATCH-20260302"),
                      ("missing_expected_batch", "BATCH-20260303"),
                      ("stale_data", "BATCH-20260303")}},
        {"name": "payment_spike", "demo": date(2026, 3, 1), "spike": True,
         "expected": {("unusual_payment_total", SPIKE_BATCH)}, "require_reconciled": True},
    ]

    results = []
    for sc in scenarios:
        source_db = None
        db = sc.get("db")
        if db is None:  # build a dedicated simulation warehouse
            db = fresh(sim / f"{sc['name']}.duckdb")
            if sc.get("spike"):
                source_db = make_spike_source(work_dir, sim / "claims_spike_source.db")
            run_load(work_dir, db, source_db=source_db, **sc.get("load", {}))

        inc = detect(work_dir, db, sc["demo"], DEFAULT_SCHEDULE, source_db, sc["name"])
        (out_dir / f"{sc['name']}.json").write_text(json.dumps(inc, indent=2) + "\n", encoding="utf-8")

        got = {(f["check"], f["batch_id"]) for f in inc["findings"]}
        missing, unexpected = sc["expected"] - got, got - sc["expected"]
        ok = not missing and not unexpected
        if sc.get("require_reconciled"):
            ok &= inc["reconciliation"]["passed"]
        if sc["name"] == "healthy":
            ok &= inc["reconciliation"]["passed"] and inc["status"] == "NO_INCIDENT"
        results.append({
            "scenario": sc["name"], "demo_date": sc["demo"].isoformat(), "duckdb": str(db),
            "reconciliation_passed": inc["reconciliation"]["passed"],
            "incident_id": inc["incident_id"], "highest_severity": inc["highest_severity"],
            "expected": sorted(map(list, sc["expected"])), "observed": sorted(map(list, got)),
            "missing_alerts": sorted(map(list, missing)), "unexpected_alerts": sorted(map(list, unexpected)),
            "findings": [{k: f[k] for k in ("finding_id", "check", "category", "batch_id",
                                            "expected", "observed", "unit", "severity")}
                         for f in inc["findings"]],
            "passed": ok,
        })

    summary = {"passed": all(r["passed"] for r in results), "scenarios": results}
    (out_dir / "validation_summary.json").write_text(json.dumps(summary, indent=2) + "\n", encoding="utf-8")

    for r in results:
        print(f"[{'PASS' if r['passed'] else 'FAIL'}] {r['scenario']:<16} demo {r['demo_date']}  "
              f"reconciliation {'PASSED' if r['reconciliation_passed'] else 'FAILED'}  "
              f"incident {r['incident_id'] or '-'}")
        for f in r["findings"]:
            print(f"        {f['finding_id']} {f['severity']:<8} {f['category']:<16} {f['check']:<22} "
                  f"{f['batch_id']}  expected {f['expected']}  observed {f['observed']} {f['unit']}")
        if r["missing_alerts"] or r["unexpected_alerts"]:
            print(f"        missing {r['missing_alerts']}  unexpected {r['unexpected_alerts']}")
    print(f"RESULT: {'ALL SCENARIOS PASSED' if summary['passed'] else 'FAILED'}")
    print(f"Incidents and summary: {out_dir}")
    sys.exit(0 if summary["passed"] else 1)


if __name__ == "__main__":
    main()
