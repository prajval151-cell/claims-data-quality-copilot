"""Aggregate pipeline results for CI logs and the GitHub job summary.

Usage:
  python scripts/ci_summary.py [--markdown-out FILE] [--append FILE]

Reads the JSON reports that the pipeline writes under CLAIMS_WORK_DIR and prints one
Markdown table of check counts and run totals. It never prints individual records: only
counts, paise totals, check pass/fail tallies and statuses.

Exit code: 0 when every required stage passed (live Claude API verification may be
PENDING), 1 when a report is missing or a stage failed.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from claims_workdir import ensure_outside_repo, get_work_dir


def load(path: Path) -> dict | None:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return None


def tally(checks: dict) -> str:
    return f"{sum(bool(v) for v in checks.values())}/{len(checks)}"


def stage_rows(w: Path) -> list[tuple[str, str, str, bool]]:
    rows: list[tuple[str, str, str, bool]] = []

    def missing(name: str, path: Path) -> None:
        rows.append((name, "MISSING", f"`{path.name}` not found", False))

    # Milestone 1: synthetic data, two runs, identical results
    for run in ("run1", "run2"):
        p = w / "validation" / f"{run}.json"
        r = load(p)
        if r is None:
            missing(f"M1 validation {run}", p)
            continue
        rc = r["row_counts"]
        rows.append((f"M1 validation {run}", "PASSED" if r["passed"] else "FAILED",
                     f"{tally(r['checks'])} checks; {rc['payments']:,} payments in {rc['load_batches']} batches; "
                     f"{r['payment_total_paise']:,} paise", r["passed"]))
    a, b = load(w / "validation" / "run1.json"), load(w / "validation" / "run2.json")
    if a and b:
        same = a["row_counts"] == b["row_counts"] and a["payment_total_paise"] == b["payment_total_paise"] \
            and a["source_fingerprint"] == b["source_fingerprint"]
        rows.append(("M1 determinism (run1 = run2)", "PASSED" if same else "FAILED",
                     "row counts, paise total and source fingerprint identical" if same else "runs differ", same))

    # Milestone 2: DuckDB load, rerun, zero-row simulation
    for name, label in (("load1", "M2 reconcile load 1"), ("load2", "M2 reconcile load 2 (rerun)"),
                        ("zero_row_sim", "M2 zero-row simulation")):
        p = w / "reports" / "milestone2" / f"{name}.json"
        r = load(p)
        if r is None:
            missing(label, p)
            continue
        t = r["totals"]
        rows.append((label, "PASSED" if r["passed"] else "FAILED",
                     f"{tally(r['checks'])} checks; source {t['source_rows']:,} / staging {t['staging_rows']:,} / "
                     f"target {t['target_rows']:,} rows", r["passed"]))
    l1, l2 = load(w / "reports" / "milestone2" / "load1.json"), load(w / "reports" / "milestone2" / "load2.json")
    if l1 and l2:
        same = l1["target_fingerprint"] == l2["target_fingerprint"] and l1["totals"] == l2["totals"]
        rows.append(("M2 rerun identical", "PASSED" if same else "FAILED",
                     "target fingerprint and totals identical" if same else "reruns differ", same))

    # Milestone 3: anomaly scenarios
    p = w / "incidents" / "milestone3" / "validation_summary.json"
    r = load(p)
    if r is None:
        missing("M3 anomaly scenarios", p)
    else:
        sc = r["scenarios"]
        findings = sum(len(s["findings"]) for s in sc)
        rows.append(("M3 anomaly scenarios", "PASSED" if r["passed"] else "FAILED",
                     f"{sum(s['passed'] for s in sc)}/{len(sc)} scenarios exact; {findings} findings total",
                     r["passed"]))

    # Milestone 4: runbook retrieval
    p = w / "reports" / "milestone4" / "verification_summary.json"
    r = load(p)
    if r is None:
        missing("M4 runbook retrieval", p)
    else:
        hits = [x for x in r["rows"] if x["retrieval"] == "EMBEDDING"]
        rows.append(("M4 runbook retrieval", "PASSED" if r["passed"] else "FAILED",
                     f"{tally(r['required_checks'])} required checks; {sum(x['passed'] for x in hits)}/{len(hits)} "
                     f"findings hit their runbook; {r['passages_indexed']} passages", r["passed"]))

    # Milestone 5: agent, mock and live
    p = w / "reports" / "milestone5" / "mock" / "verification_mock.json"
    r = load(p)
    if r is None:
        missing("M5 agent - MOCK verification", p)
    else:
        n_checks = sum(len(c["checks"]) for c in r["cases"]) + len(r["server_and_data_checks"])
        n_pass = sum(sum(map(bool, c["checks"].values())) for c in r["cases"]) + \
            sum(map(bool, r["server_and_data_checks"].values()))
        rows.append(("M5 agent - MOCK verification", r["status"],
                     f"{n_pass}/{n_checks} checks across {len(r['cases'])} cases; no Claude API call", r["passed"]))
    r = load(w / "reports" / "milestone5" / "live" / "verification_live.json")
    if r is None:
        rows.append(("M5 agent - LIVE Claude API verification", "PENDING", "not run", True))
    elif r["status"] == "PENDING":
        rows.append(("M5 agent - LIVE Claude API verification", "PENDING", r["reason"], True))
    else:
        rows.append(("M5 agent - LIVE Claude API verification", r["status"],
                     f"{len(r['cases'])} cases via the Claude API", r["passed"]))
    return rows


def render(rows: list[tuple[str, str, str, bool]]) -> str:
    out = ["## Claims Data Quality Copilot - pipeline results", "",
           "| Stage | Status | Aggregate result |", "|---|---|---|"]
    out += [f"| {name} | **{status}** | {detail} |" for name, status, detail, _ in rows]
    ok = all(r[3] for r in rows)
    out += ["", f"Overall: **{'PASSED' if ok else 'FAILED'}** "
                "(LIVE Claude API verification may be PENDING without failing the run)", ""]
    return "\n".join(out)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--markdown-out", type=Path, help="write the summary here (must be outside the repo)")
    parser.add_argument("--append", type=Path, help="append the summary to this file (e.g. $GITHUB_STEP_SUMMARY)")
    args = parser.parse_args()
    rows = stage_rows(get_work_dir(create=False))
    text = render(rows)
    print(text)
    if args.markdown_out:
        ensure_outside_repo(args.markdown_out, "--markdown-out").write_text(text + "\n", encoding="utf-8")
    if args.append:
        with open(args.append, "a", encoding="utf-8") as f:
            f.write(text + "\n")
    sys.exit(0 if all(r[3] for r in rows) else 1)


if __name__ == "__main__":
    main()
