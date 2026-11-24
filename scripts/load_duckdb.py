"""Load the 60 payment batches from SQLite into DuckDB via staging.

Source : $CLAIMS_WORK_DIR\\claims.db                     (opened read-only)
Target : $CLAIMS_WORK_DIR\\warehouse\\claims.duckdb       (default; override with --db)

For each batch, in one transaction:
  1. clear stg_claim_payment and load that batch's source rows into it
  2. delete the batch from fact_claim_payment and insert it again from staging
  3. record source, staging, and target row counts and paise totals in etl_batch_audit

Because each batch is replaced as a unit, rerunning the load never duplicates
rows (fact_claim_payment.payment_id is also a primary key).

Simulations (each requires a separate --db file so the main warehouse is never
affected):
  --simulate-drop-batch BATCH_ID          staging filter drops every row of the batch
  --simulate-drop-rows BATCH_ID:N         staging loses the last N rows of the batch
  --simulate-amount-delta BATCH_ID:PAISE  staging adds PAISE to one row of the batch
  --source-db FILE                        load from another SQLite file (e.g. a spike copy)
"""

from __future__ import annotations

import argparse
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

from claims_workdir import (
    db_path,
    duckdb_connect,
    duckdb_path,
    ensure_outside_repo,
    get_work_dir,
    sqlite_readonly,
)

PAYMENT_COLUMNS = ["payment_id", "batch_id", "claim_id", "payment_date", "amount_paise",
                   "payment_method", "payee_type", "transaction_ref"]

DDL = """
CREATE TABLE IF NOT EXISTS etl_load_run (
    run_id          BIGINT PRIMARY KEY,
    started_at      TIMESTAMP NOT NULL,
    finished_at     TIMESTAMP,
    source_db       VARCHAR NOT NULL,
    staging_filter  VARCHAR,
    status          VARCHAR NOT NULL
);

CREATE TABLE IF NOT EXISTS stg_claim_payment (
    payment_id      VARCHAR,
    batch_id        VARCHAR,
    claim_id        VARCHAR,
    payment_date    DATE,
    amount_paise    BIGINT,
    payment_method  VARCHAR,
    payee_type      VARCHAR,
    transaction_ref VARCHAR
);

CREATE TABLE IF NOT EXISTS fact_claim_payment (
    payment_id      VARCHAR PRIMARY KEY,
    batch_id        VARCHAR NOT NULL,
    claim_id        VARCHAR NOT NULL,
    payment_date    DATE    NOT NULL,
    amount_paise    BIGINT  NOT NULL CHECK (amount_paise > 0),
    payment_method  VARCHAR NOT NULL,
    payee_type      VARCHAR NOT NULL,
    transaction_ref VARCHAR NOT NULL,
    load_run_id     BIGINT  NOT NULL
);

CREATE TABLE IF NOT EXISTS etl_batch_audit (
    run_id               BIGINT  NOT NULL,
    batch_id             VARCHAR NOT NULL,
    batch_date           DATE    NOT NULL,
    source_rows          BIGINT  NOT NULL,
    source_amount_paise  BIGINT  NOT NULL,
    staging_rows         BIGINT  NOT NULL,
    staging_amount_paise BIGINT  NOT NULL,
    target_rows          BIGINT  NOT NULL,
    target_amount_paise  BIGINT  NOT NULL,
    PRIMARY KEY (run_id, batch_id)
);
"""

STAGE_INSERT = """
INSERT INTO stg_claim_payment
SELECT unnest($payment_id), unnest($batch_id), unnest($claim_id),
       CAST(unnest($payment_date) AS DATE), CAST(unnest($amount_paise) AS BIGINT),
       unnest($payment_method), unnest($payee_type), unnest($transaction_ref)
"""


def utcnow() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


def parse_fault(value: str | None, flag: str) -> tuple[str, int] | None:
    if not value:
        return None
    batch, _, number = value.partition(":")
    try:
        return batch, int(number)
    except ValueError:
        sys.exit(f"ERROR: {flag} expects BATCH_ID:INTEGER, got {value!r}")


def run_load(work_dir: Path, target_db: Path, source_db: Path | None = None,
             drop_batch: str | None = None, drop_rows: tuple[str, int] | None = None,
             amount_delta: tuple[str, int] | None = None) -> dict:
    """Load every source batch into target_db. Fault arguments simulate staging defects."""
    main_db = duckdb_path(work_dir)
    simulated = bool(drop_batch or drop_rows or amount_delta or source_db)
    if simulated and os.path.normcase(str(target_db)) == os.path.normcase(str(main_db)):
        sys.exit("ERROR: simulations and --source-db must use a separate --db file, not the main warehouse.")

    src = sqlite_readonly(work_dir, source_db)
    source_label = str(source_db or db_path(work_dir))
    batches = src.execute("SELECT batch_id, batch_date FROM load_batches ORDER BY batch_date").fetchall()
    known = {b for b, _ in batches}
    for batch in [drop_batch, drop_rows and drop_rows[0], amount_delta and amount_delta[0]]:
        if batch and batch not in known:
            sys.exit(f"ERROR: unknown batch {batch}")

    faults = []
    if drop_batch:
        faults.append(f"batch_id <> '{drop_batch}'")
    if drop_rows:
        faults.append(f"drop last {drop_rows[1]} rows of {drop_rows[0]}")
    if amount_delta:
        faults.append(f"add {amount_delta[1]} paise to first row of {amount_delta[0]}")
    staging_filter = "; ".join(faults) or None

    con = duckdb_connect(target_db, work_dir)
    con.execute(DDL)
    run_id = con.execute("SELECT COALESCE(MAX(run_id), 0) + 1 FROM etl_load_run").fetchone()[0]
    con.execute("INSERT INTO etl_load_run VALUES (?, ?, NULL, ?, ?, 'RUNNING')",
                [run_id, utcnow(), source_label, staging_filter])

    cols = ", ".join(PAYMENT_COLUMNS)
    for batch_id, batch_date in batches:
        rows = src.execute(f"SELECT {cols} FROM payments WHERE batch_id = ? ORDER BY payment_id",
                           [batch_id]).fetchall()
        source_rows = len(rows)
        source_amount = sum(r[4] for r in rows)
        # Simulated staging defects (only ever applied to a separate --db file).
        if drop_batch == batch_id:
            rows = []
        if drop_rows and drop_rows[0] == batch_id:
            rows = rows[:max(0, len(rows) - drop_rows[1])]
        if amount_delta and amount_delta[0] == batch_id and rows:
            first = list(rows[0])
            first[4] += amount_delta[1]
            rows = [tuple(first), *rows[1:]]

        con.execute("BEGIN TRANSACTION")
        try:
            con.execute("DELETE FROM stg_claim_payment")
            if rows:
                con.execute(STAGE_INSERT, {c: [r[i] for r in rows] for i, c in enumerate(PAYMENT_COLUMNS)})
            staging_rows, staging_amount = con.execute(
                "SELECT COUNT(*), CAST(COALESCE(SUM(amount_paise), 0) AS BIGINT) FROM stg_claim_payment"
            ).fetchone()
            con.execute("DELETE FROM fact_claim_payment WHERE batch_id = ?", [batch_id])
            con.execute(f"INSERT INTO fact_claim_payment SELECT {cols}, ? FROM stg_claim_payment", [run_id])
            target_rows, target_amount = con.execute(
                "SELECT COUNT(*), CAST(COALESCE(SUM(amount_paise), 0) AS BIGINT) "
                "FROM fact_claim_payment WHERE batch_id = ?", [batch_id]
            ).fetchone()
            con.execute("INSERT INTO etl_batch_audit VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                        [run_id, batch_id, batch_date, source_rows, source_amount,
                         staging_rows, staging_amount, target_rows, target_amount])
            con.execute("COMMIT")
        except Exception:
            con.execute("ROLLBACK")
            con.execute("UPDATE etl_load_run SET status = 'FAILED', finished_at = ? WHERE run_id = ?",
                        [utcnow(), run_id])
            raise

    con.execute("DELETE FROM stg_claim_payment")
    con.execute("UPDATE etl_load_run SET status = 'COMPLETED', finished_at = ? WHERE run_id = ?",
                [utcnow(), run_id])
    totals = con.execute(
        "SELECT COUNT(*), SUM(source_rows), SUM(staging_rows), SUM(target_rows), "
        "CAST(SUM(source_amount_paise) AS BIGINT), CAST(SUM(target_amount_paise) AS BIGINT) "
        "FROM etl_batch_audit WHERE run_id = ?", [run_id]).fetchone()
    fact_rows = con.execute("SELECT COUNT(*) FROM fact_claim_payment").fetchone()[0]
    con.close()
    src.close()
    return {"db": str(target_db), "run_id": run_id, "staging_filter": staging_filter,
            "batches": totals[0], "source_rows": totals[1], "staging_rows": totals[2],
            "target_rows": totals[3], "source_paise": totals[4], "target_paise": totals[5],
            "fact_rows": fact_rows}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--db", type=Path, help="DuckDB file (default: $CLAIMS_WORK_DIR\\warehouse\\claims.duckdb)")
    parser.add_argument("--source-db", type=Path, help="alternative SQLite source (needs a separate --db)")
    parser.add_argument("--simulate-drop-batch", metavar="BATCH_ID",
                        help="zero-row simulation: staging filter drops this batch (needs a separate --db)")
    parser.add_argument("--simulate-drop-rows", metavar="BATCH_ID:N",
                        help="partial load: staging loses the last N rows of the batch")
    parser.add_argument("--simulate-amount-delta", metavar="BATCH_ID:PAISE",
                        help="amount drift: staging adds PAISE to one row of the batch")
    args = parser.parse_args()

    work_dir = get_work_dir(create=False)
    target_db = ensure_outside_repo(args.db, "--db") if args.db else duckdb_path(work_dir)
    r = run_load(work_dir, target_db, args.source_db, args.simulate_drop_batch,
                 parse_fault(args.simulate_drop_rows, "--simulate-drop-rows"),
                 parse_fault(args.simulate_amount_delta, "--simulate-amount-delta"))
    print(f"DuckDB        : {r['db']}")
    print(f"Run ID        : {r['run_id']}" + (f"  (staging filter: {r['staging_filter']})" if r["staging_filter"] else ""))
    print(f"Batches       : {r['batches']}")
    print(f"Rows          : source {r['source_rows']:,} | staging {r['staging_rows']:,} | target {r['target_rows']:,}")
    print(f"Amount (paise): source {r['source_paise']:,} | target {r['target_paise']:,}")
    print(f"fact rows now : {r['fact_rows']:,}")


if __name__ == "__main__":
    main()
