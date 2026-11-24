"""Validate the generated source data and the loaded SQLite database.

Usage:
  python scripts/validate_data.py [--output REPORT.json]
  python scripts/validate_data.py --compare RUN1.json RUN2.json

The first form runs all checks against $CLAIMS_WORK_DIR, prints aggregate
results, optionally saves them as JSON, and exits non-zero if any check fails.
The second form compares two saved reports and fails unless row counts,
payment totals, and file fingerprints are identical.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import sqlite3
import sys
from pathlib import Path

from claims_workdir import db_path, get_work_dir, manifest_path, payments_dir, source_dir

EXPECTED_BATCHES = 60
EXPECTED_PER_BATCH = 1_500
EXPECTED_PAYMENTS = 90_000


def scalar(conn: sqlite3.Connection, sql: str) -> int:
    return conn.execute(sql).fetchone()[0]


def csv_stats(work_dir: Path) -> dict:
    files = sorted(payments_dir(work_dir).glob("payments_*.csv"))
    per_file = {}
    ids: set[str] = set()
    total_rows = total_paise = 0
    for path in files:
        with path.open(newline="", encoding="utf-8") as f:
            rows = list(csv.DictReader(f))
        per_file[path.name] = len(rows)
        total_rows += len(rows)
        total_paise += sum(int(r["amount_paise"]) for r in rows)
        ids.update(r["payment_id"] for r in rows)
    return {
        "batch_files": len(files),
        "rows_per_file": sorted(set(per_file.values())),
        "total_rows": total_rows,
        "distinct_payment_ids": len(ids),
        "total_paise": total_paise,
    }


def source_fingerprint(work_dir: Path) -> str:
    """SHA-256 over every source file's path and hash, in manifest order."""
    manifest = json.loads(manifest_path(work_dir).read_text(encoding="utf-8"))
    h = hashlib.sha256()
    for entry in manifest["files"]:
        h.update(f"{entry['path']}:{entry['sha256']}\n".encode())
    return h.hexdigest()


def run_checks(work_dir: Path) -> dict:
    csvs = csv_stats(work_dir)
    conn = sqlite3.connect(f"file:{db_path(work_dir)}?mode=ro", uri=True)
    try:
        counts = {t: scalar(conn, f"SELECT COUNT(*) FROM {t}")
                  for t in ("policies", "claims", "load_batches", "payments")}
        batch_sizes = [r[0] for r in conn.execute(
            "SELECT COUNT(*) FROM payments GROUP BY batch_id")]
        db_total = scalar(conn, "SELECT COALESCE(SUM(amount_paise), 0) FROM payments")

        checks = {
            "csv_batch_files == 60": csvs["batch_files"] == EXPECTED_BATCHES,
            "csv_rows_per_file == 1500": csvs["rows_per_file"] == [EXPECTED_PER_BATCH],
            "csv_total_rows == 90000": csvs["total_rows"] == EXPECTED_PAYMENTS,
            "csv_payment_ids_unique": csvs["distinct_payment_ids"] == csvs["total_rows"],
            "db_load_batches == 60": counts["load_batches"] == EXPECTED_BATCHES,
            "db_payment_batches == 60": len(batch_sizes) == EXPECTED_BATCHES,
            "db_rows_per_batch == 1500": set(batch_sizes) == {EXPECTED_PER_BATCH},
            "db_load_batches.row_count == 1500": scalar(conn,
                "SELECT COUNT(*) FROM load_batches WHERE row_count != 1500") == 0,
            "db_total_payments == 90000": counts["payments"] == EXPECTED_PAYMENTS,
            "db_payment_ids_unique": scalar(conn,
                "SELECT COUNT(DISTINCT payment_id) FROM payments") == counts["payments"],
            "csv_rows == db_rows": csvs["total_rows"] == counts["payments"],
            "csv_total_paise == db_total_paise": csvs["total_paise"] == db_total,
            "amounts_are_positive_integers": scalar(conn,
                "SELECT COUNT(*) FROM payments WHERE typeof(amount_paise) != 'integer' "
                "OR amount_paise <= 0") == 0,
            "payments -> claims (no orphans)": scalar(conn,
                "SELECT COUNT(*) FROM payments p LEFT JOIN claims c USING (claim_id) "
                "WHERE c.claim_id IS NULL") == 0,
            "payments -> load_batches (no orphans)": scalar(conn,
                "SELECT COUNT(*) FROM payments p LEFT JOIN load_batches b USING (batch_id) "
                "WHERE b.batch_id IS NULL") == 0,
            "claims -> policies (no orphans)": scalar(conn,
                "SELECT COUNT(*) FROM claims c LEFT JOIN policies p USING (policy_id) "
                "WHERE p.policy_id IS NULL") == 0,
            "sqlite foreign_key_check clean": not conn.execute("PRAGMA foreign_key_check").fetchall(),
            "payment_date matches batch date": scalar(conn,
                "SELECT COUNT(*) FROM payments p JOIN load_batches b USING (batch_id) "
                "WHERE p.payment_date != b.batch_date") == 0,
            "payments only on APPROVED claims": scalar(conn,
                "SELECT COUNT(*) FROM payments p JOIN claims c USING (claim_id) "
                "WHERE c.claim_status != 'APPROVED'") == 0,
            "payment_date >= claim_date": scalar(conn,
                "SELECT COUNT(*) FROM payments p JOIN claims c USING (claim_id) "
                "WHERE p.payment_date < c.claim_date") == 0,
            "claim_date within policy period": scalar(conn,
                "SELECT COUNT(*) FROM claims c JOIN policies p USING (policy_id) "
                "WHERE c.claim_date NOT BETWEEN p.start_date AND p.end_date") == 0,
            "paid total == approved amount per paid claim": scalar(conn,
                "SELECT COUNT(*) FROM (SELECT claim_id, SUM(amount_paise) AS paid "
                "FROM payments GROUP BY claim_id) s JOIN claims c USING (claim_id) "
                "WHERE s.paid != c.approved_amount_paise") == 0,
        }
        paid_claims = scalar(conn, "SELECT COUNT(DISTINCT claim_id) FROM payments")
    finally:
        conn.close()

    return {
        "work_dir": str(work_dir),
        "row_counts": {**counts, "csv_payment_rows": csvs["total_rows"]},
        "payment_total_paise": db_total,
        "csv_payment_total_paise": csvs["total_paise"],
        "paid_claims": paid_claims,
        "source_fingerprint": source_fingerprint(work_dir),
        "checks": checks,
        "passed": all(checks.values()),
    }


def print_report(report: dict) -> None:
    rc = report["row_counts"]
    total = report["payment_total_paise"]
    print(f"Work dir            : {report['work_dir']}")
    print(f"Policies            : {rc['policies']:,}")
    print(f"Claims              : {rc['claims']:,} ({report['paid_claims']:,} with payments)")
    print(f"Payment batches     : {rc['load_batches']}")
    print(f"Payments            : {rc['payments']:,}")
    print(f"Payment total       : {total:,} paise (Rs {total // 100:,}.{total % 100:02d})")
    print(f"Source fingerprint  : {report['source_fingerprint']}")
    print("Checks:")
    for name, ok in report["checks"].items():
        print(f"  [{'PASS' if ok else 'FAIL'}] {name}")
    print(f"RESULT: {'ALL CHECKS PASSED' if report['passed'] else 'FAILED'}")


def compare(a_path: Path, b_path: Path) -> bool:
    a = json.loads(a_path.read_text(encoding="utf-8"))
    b = json.loads(b_path.read_text(encoding="utf-8"))
    fields = [
        ("row_counts", a["row_counts"], b["row_counts"]),
        ("payment_total_paise", a["payment_total_paise"], b["payment_total_paise"]),
        ("source_fingerprint", a["source_fingerprint"], b["source_fingerprint"]),
        ("both runs passed all checks", a["passed"] and b["passed"], True),
    ]
    ok = True
    print(f"Comparing {a_path.name} vs {b_path.name}")
    for name, va, vb in fields:
        same = va == vb
        ok &= same
        shown = json.dumps(va) if not isinstance(va, str) else va
        print(f"  [{'SAME' if same else 'DIFF'}] {name}: {shown}" + ("" if same else f" vs {vb}"))
    print(f"RESULT: {'RUNS ARE IDENTICAL' if ok else 'RUNS DIFFER'}")
    return ok


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--output", type=Path, help="write the report as JSON to this path")
    parser.add_argument("--compare", nargs=2, type=Path, metavar=("RUN1", "RUN2"))
    args = parser.parse_args()

    if args.compare:
        sys.exit(0 if compare(*args.compare) else 1)

    work_dir = get_work_dir(create=False)
    if not db_path(work_dir).is_file() or not source_dir(work_dir).is_dir():
        sys.exit("ERROR: run generate_data.py and load_sqlite.py first.")
    report = run_checks(work_dir)
    print_report(report)
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
        print(f"Report saved to {args.output}")
    sys.exit(0 if report["passed"] else 1)


if __name__ == "__main__":
    main()
