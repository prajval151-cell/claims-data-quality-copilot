"""Reconcile SQLite source payments against DuckDB fact_claim_payment.

Usage:
  python scripts/reconcile_duckdb.py [--db FILE] [--output REPORT.json] [--expect-dropped-batch BATCH_ID]
  python scripts/reconcile_duckdb.py --compare RUN1.json RUN2.json

Both databases are opened read-only. Checks:
  * full-row comparison in both directions (source rows missing from the
    target, target rows not in the source), over all 8 payment columns
  * key comparison in both directions, plus per-column mismatch counts for
    payment IDs present on both sides
  * duplicate payment IDs, counted separately in source and target
  * per-batch and full-run row counts and exact integer-paise totals for
    source, staging (from the latest load's audit), and target

Without --expect-dropped-batch, the run passes only if every difference is zero.
With it, the run passes only if exactly that batch is missing from the target
(source rows > 0, staging and target 0) and everything else matches.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from collections import Counter
from datetime import date
from pathlib import Path

from claims_workdir import (
    duckdb_connect,
    duckdb_path,
    ensure_outside_repo,
    get_work_dir,
    sqlite_readonly,
)

COLUMNS = ["payment_id", "batch_id", "claim_id", "payment_date", "amount_paise",
           "payment_method", "payee_type", "transaction_ref"]
AMOUNT = COLUMNS.index("amount_paise")
BATCH = COLUMNS.index("batch_id")


def normalise(row: tuple) -> tuple:
    """Make SQLite (TEXT dates) and DuckDB (DATE) values comparable without losing precision."""
    return tuple(v.isoformat() if isinstance(v, date) else v for v in row)


def batch_stats(rows: list[tuple]) -> dict[str, list[int]]:
    stats: dict[str, list[int]] = {}
    for r in rows:
        s = stats.setdefault(r[BATCH], [0, 0])
        s[0] += 1
        s[1] += r[AMOUNT]
    return stats


def reconcile(work_dir: Path, db: Path, source_db: Path | None = None) -> dict:
    cols = ", ".join(COLUMNS)
    src = sqlite_readonly(work_dir, source_db)
    source = [normalise(r) for r in src.execute(f"SELECT {cols} FROM payments")]
    batches = src.execute("SELECT batch_id FROM load_batches ORDER BY batch_date").fetchall()
    batches = [b for (b,) in batches]
    src.close()

    tgt = duckdb_connect(db, work_dir, read_only=True)
    target = [normalise(r) for r in tgt.execute(f"SELECT {cols} FROM fact_claim_payment").fetchall()]
    amount_type = tgt.execute(
        "SELECT data_type FROM information_schema.columns "
        "WHERE table_name = 'fact_claim_payment' AND column_name = 'amount_paise'").fetchone()[0]
    run_id, run_status, staging_filter = tgt.execute(
        "SELECT run_id, status, staging_filter FROM etl_load_run ORDER BY run_id DESC LIMIT 1").fetchone()
    load_runs = tgt.execute("SELECT COUNT(*) FROM etl_load_run").fetchone()[0]
    audit = {b: (sr, sa) for b, sr, sa in tgt.execute(
        "SELECT batch_id, staging_rows, staging_amount_paise FROM etl_batch_audit WHERE run_id = ?",
        [run_id]).fetchall()}
    tgt.close()

    # Duplicate payment IDs (counted separately so they cannot hide row differences).
    src_ids = Counter(r[0] for r in source)
    tgt_ids = Counter(r[0] for r in target)
    duplicates = {
        "source_duplicate_payment_ids": sum(1 for n in src_ids.values() if n > 1),
        "source_extra_rows_from_duplicates": sum(n - 1 for n in src_ids.values() if n > 1),
        "target_duplicate_payment_ids": sum(1 for n in tgt_ids.values() if n > 1),
        "target_extra_rows_from_duplicates": sum(n - 1 for n in tgt_ids.values() if n > 1),
    }

    # Full-row comparison in both directions (multiset, all columns).
    src_rows, tgt_rows = Counter(source), Counter(target)
    source_not_in_target = src_rows - tgt_rows
    target_not_in_source = tgt_rows - src_rows

    # Key comparison in both directions and per-column mismatches for shared keys.
    missing_ids = src_ids.keys() - tgt_ids.keys()
    extra_ids = tgt_ids.keys() - src_ids.keys()
    src_by_id = {r[0]: r for r in source}
    tgt_by_id = {r[0]: r for r in target}
    column_mismatches = dict.fromkeys(COLUMNS[1:], 0)
    for pid in src_by_id.keys() & tgt_by_id.keys():
        a, b = src_by_id[pid], tgt_by_id[pid]
        for i, c in enumerate(COLUMNS[1:], start=1):
            if a[i] != b[i]:
                column_mismatches[c] += 1

    # Per-batch counts and exact paise totals.
    s_stats, t_stats = batch_stats(source), batch_stats(target)
    missing_by_batch = Counter(src_by_id[p][BATCH] for p in missing_ids)
    per_batch = []
    for b in batches:
        sr, sa = s_stats.get(b, [0, 0])
        gr, ga = audit.get(b, (None, None))
        tr, ta = t_stats.get(b, [0, 0])
        per_batch.append({
            "batch_id": b,
            "source_rows": sr, "staging_rows": gr, "target_rows": tr,
            "source_amount_paise": sa, "staging_amount_paise": ga, "target_amount_paise": ta,
            "row_diff": tr - sr, "amount_diff_paise": ta - sa,
            "missing_in_target": missing_by_batch.get(b, 0),
            "status": "MATCH" if (sr, sa) == (tr, ta) == (gr, ga) else "MISMATCH",
        })
    unknown_target_batches = sorted(t_stats.keys() - set(batches))

    totals = {
        "batches": len(batches),
        "source_rows": len(source),
        "staging_rows": sum(p["staging_rows"] or 0 for p in per_batch),
        "target_rows": len(target),
        "source_amount_paise": sum(r[AMOUNT] for r in source),
        "staging_amount_paise": sum(p["staging_amount_paise"] or 0 for p in per_batch),
        "target_amount_paise": sum(r[AMOUNT] for r in target),
    }
    totals["row_diff"] = totals["target_rows"] - totals["source_rows"]
    totals["amount_diff_paise"] = totals["target_amount_paise"] - totals["source_amount_paise"]

    differences = {
        "source_rows_not_in_target": sum(source_not_in_target.values()),
        "target_rows_not_in_source": sum(target_not_in_source.values()),
        "source_ids_missing_in_target": len(missing_ids),
        "target_ids_not_in_source": len(extra_ids),
        "column_mismatches": column_mismatches,
        "mismatched_batches": [p["batch_id"] for p in per_batch if p["status"] != "MATCH"],
        "target_batches_not_in_source": unknown_target_batches,
    }

    h = hashlib.sha256()
    for r in sorted(target):
        h.update(("|".join(map(str, r)) + "\n").encode())

    return {
        "duckdb": str(db),
        "source_db": str(source_db) if source_db else "claims.db (Milestone 1)",
        "latest_load_run": {"run_id": run_id, "status": run_status, "staging_filter": staging_filter},
        "load_runs_in_db": load_runs,
        "target_amount_type": amount_type,
        "totals": totals,
        "duplicates": duplicates,
        "differences": differences,
        "per_batch": per_batch,
        "target_fingerprint": h.hexdigest(),
    }


def evaluate(report: dict, expect_dropped: str | None) -> dict[str, bool]:
    t, d, dup = report["totals"], report["differences"], report["duplicates"]
    common = {
        "latest load run COMPLETED": report["latest_load_run"]["status"] == "COMPLETED",
        "amount column is BIGINT (integer paise)": report["target_amount_type"] == "BIGINT",
        "zero duplicate payment IDs (source)": dup["source_duplicate_payment_ids"] == 0,
        "zero duplicate payment IDs (target)": dup["target_duplicate_payment_ids"] == 0,
        "zero target rows not in source": d["target_rows_not_in_source"] == 0,
        "zero target IDs not in source": d["target_ids_not_in_source"] == 0,
        "zero column mismatches": not any(d["column_mismatches"].values()),
        "zero unknown target batches": not d["target_batches_not_in_source"],
    }
    if not expect_dropped:
        return {**common,
                "zero source rows not in target": d["source_rows_not_in_target"] == 0,
                "zero source IDs missing in target": d["source_ids_missing_in_target"] == 0,
                "all batches MATCH (source = staging = target)": not d["mismatched_batches"],
                "full-run rows equal": t["row_diff"] == 0 and t["staging_rows"] == t["source_rows"],
                "full-run paise totals equal": t["amount_diff_paise"] == 0
                and t["staging_amount_paise"] == t["source_amount_paise"]}

    dropped = next((p for p in report["per_batch"] if p["batch_id"] == expect_dropped), None)
    return {**common,
            f"{expect_dropped} reported: source > 0, staging 0, target 0":
                dropped is not None and dropped["source_rows"] > 0
                and dropped["staging_rows"] == 0 and dropped["target_rows"] == 0,
            "only that batch mismatched": d["mismatched_batches"] == [expect_dropped],
            "missing rows == that batch's source rows":
                dropped is not None
                and d["source_rows_not_in_target"] == dropped["source_rows"]
                and d["source_ids_missing_in_target"] == dropped["source_rows"]
                and dropped["missing_in_target"] == dropped["source_rows"]}


def print_report(report: dict, checks: dict[str, bool]) -> None:
    t, d, dup = report["totals"], report["differences"], report["duplicates"]
    lr = report["latest_load_run"]
    print(f"DuckDB            : {report['duckdb']}")
    print(f"Latest load run   : {lr['run_id']} {lr['status']}"
          + (f" (staging filter: {lr['staging_filter']})" if lr["staging_filter"] else ""))
    print(f"Batches           : {t['batches']}")
    print(f"Rows              : source {t['source_rows']:,} | staging {t['staging_rows']:,} | "
          f"target {t['target_rows']:,} | diff {t['row_diff']:,}")
    print(f"Amount (paise)    : source {t['source_amount_paise']:,} | staging {t['staging_amount_paise']:,} | "
          f"target {t['target_amount_paise']:,} | diff {t['amount_diff_paise']:,}")
    print(f"Duplicate IDs     : source {dup['source_duplicate_payment_ids']} | "
          f"target {dup['target_duplicate_payment_ids']}")
    print(f"Source -> target  : {d['source_rows_not_in_target']:,} rows not in target, "
          f"{d['source_ids_missing_in_target']:,} IDs missing")
    print(f"Target -> source  : {d['target_rows_not_in_source']:,} rows not in source, "
          f"{d['target_ids_not_in_source']:,} IDs extra")
    print(f"Column mismatches : {sum(d['column_mismatches'].values())} "
          f"({', '.join(f'{k}={v}' for k, v in d['column_mismatches'].items())})")
    bad = [p for p in report["per_batch"] if p["status"] != "MATCH"]
    print(f"Per-batch         : {len(report['per_batch']) - len(bad)} MATCH, {len(bad)} MISMATCH")
    for p in bad:
        print(f"  {p['batch_id']}: source {p['source_rows']} / staging {p['staging_rows']} / "
              f"target {p['target_rows']} rows; source {p['source_amount_paise']:,} / "
              f"target {p['target_amount_paise']:,} paise")
    print(f"Target fingerprint: {report['target_fingerprint']}")
    print("Checks:")
    for name, ok in checks.items():
        print(f"  [{'PASS' if ok else 'FAIL'}] {name}")
    print(f"RESULT: {'PASSED' if all(checks.values()) else 'FAILED'}")


def compare(a_path: Path, b_path: Path) -> bool:
    a = json.loads(a_path.read_text(encoding="utf-8"))
    b = json.loads(b_path.read_text(encoding="utf-8"))
    items = [
        ("totals", a["totals"], b["totals"]),
        ("duplicates", a["duplicates"], b["duplicates"]),
        ("differences", a["differences"], b["differences"]),
        ("per_batch counts and paise", a["per_batch"], b["per_batch"]),
        ("target_fingerprint", a["target_fingerprint"], b["target_fingerprint"]),
        ("both runs passed", a["passed"] and b["passed"], True),
    ]
    print(f"Comparing {a_path.name} (load run {a['latest_load_run']['run_id']}) vs "
          f"{b_path.name} (load run {b['latest_load_run']['run_id']})")
    ok = True
    for name, va, vb in items:
        same = va == vb
        ok &= same
        print(f"  [{'SAME' if same else 'DIFF'}] {name}")
    print(f"  target rows: {a['totals']['target_rows']:,} -> {b['totals']['target_rows']:,}; "
          f"target paise: {a['totals']['target_amount_paise']:,} -> {b['totals']['target_amount_paise']:,}")
    print(f"RESULT: {'RUNS ARE IDENTICAL' if ok else 'RUNS DIFFER'}")
    return ok


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--db", type=Path, help="DuckDB file (default: $CLAIMS_WORK_DIR\\warehouse\\claims.duckdb)")
    parser.add_argument("--source-db", type=Path, help="alternative SQLite source")
    parser.add_argument("--output", type=Path, help="write the JSON report here")
    parser.add_argument("--expect-dropped-batch", metavar="BATCH_ID")
    parser.add_argument("--compare", nargs=2, type=Path, metavar=("RUN1", "RUN2"))
    args = parser.parse_args()

    if args.compare:
        sys.exit(0 if compare(*args.compare) else 1)

    work_dir = get_work_dir(create=False)
    db = ensure_outside_repo(args.db, "--db") if args.db else duckdb_path(work_dir)
    report = reconcile(work_dir, db, args.source_db)
    checks = evaluate(report, args.expect_dropped_batch)
    report["expect_dropped_batch"] = args.expect_dropped_batch
    report["checks"] = checks
    report["passed"] = all(checks.values())
    print_report(report, checks)
    if args.output:
        out = ensure_outside_repo(args.output, "--output")
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
        print(f"Report saved to {out}")
    sys.exit(0 if report["passed"] else 1)


if __name__ == "__main__":
    main()
