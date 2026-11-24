"""Load the generated CSVs from $CLAIMS_WORK_DIR/source into $CLAIMS_WORK_DIR/claims.db.

The database is rebuilt from scratch on every run: it is written to a
temporary file and swapped into place only after all rows load and the
foreign-key and integrity checks pass. Each file's SHA-256 is checked
against manifest.json before loading.
"""

from __future__ import annotations

import csv
import json
import os
import sqlite3
import sys
from pathlib import Path

from claims_workdir import db_path, get_work_dir, manifest_path, sha256_file, source_dir

SCHEMA = """
CREATE TABLE policies (
    policy_id            TEXT PRIMARY KEY,
    product_type         TEXT NOT NULL,
    state_code           TEXT NOT NULL,
    start_date           TEXT NOT NULL,
    end_date             TEXT NOT NULL,
    sum_insured_paise    INTEGER NOT NULL CHECK (sum_insured_paise > 0),
    annual_premium_paise INTEGER NOT NULL CHECK (annual_premium_paise > 0),
    CHECK (end_date >= start_date)
) STRICT;

CREATE TABLE claims (
    claim_id              TEXT PRIMARY KEY,
    policy_id             TEXT NOT NULL REFERENCES policies(policy_id),
    claim_date            TEXT NOT NULL,
    claim_type            TEXT NOT NULL,
    claim_status          TEXT NOT NULL CHECK (claim_status IN ('APPROVED', 'REJECTED', 'PENDING')),
    claimed_amount_paise  INTEGER NOT NULL CHECK (claimed_amount_paise > 0),
    approved_amount_paise INTEGER NOT NULL CHECK (approved_amount_paise >= 0),
    CHECK (approved_amount_paise <= claimed_amount_paise)
) STRICT;

CREATE TABLE load_batches (
    batch_id   TEXT PRIMARY KEY,
    batch_date TEXT NOT NULL UNIQUE,
    file_name  TEXT NOT NULL UNIQUE,
    row_count  INTEGER NOT NULL,
    sha256     TEXT NOT NULL
) STRICT;

CREATE TABLE payments (
    payment_id      TEXT PRIMARY KEY,
    batch_id        TEXT NOT NULL REFERENCES load_batches(batch_id),
    claim_id        TEXT NOT NULL REFERENCES claims(claim_id),
    payment_date    TEXT NOT NULL,
    amount_paise    INTEGER NOT NULL CHECK (amount_paise > 0),
    payment_method  TEXT NOT NULL,
    payee_type      TEXT NOT NULL,
    transaction_ref TEXT NOT NULL
) STRICT;

CREATE INDEX idx_claims_policy ON claims(policy_id);
CREATE INDEX idx_payments_claim ON payments(claim_id);
CREATE INDEX idx_payments_batch ON payments(batch_id);
"""

INT_COLUMNS = {
    "sum_insured_paise", "annual_premium_paise", "claimed_amount_paise",
    "approved_amount_paise", "amount_paise",
}


def read_rows(path: Path) -> tuple[list[str], list[tuple]]:
    with path.open(newline="", encoding="utf-8") as f:
        reader = csv.reader(f)
        header = next(reader)
        int_idx = {i for i, col in enumerate(header) if col in INT_COLUMNS}
        rows = [tuple(int(v) if i in int_idx else v for i, v in enumerate(r)) for r in reader]
    return header, rows


def insert(conn: sqlite3.Connection, table: str, header: list[str], rows: list[tuple]) -> None:
    cols = ", ".join(header)
    marks = ", ".join("?" for _ in header)
    conn.executemany(f"INSERT INTO {table} ({cols}) VALUES ({marks})", rows)


def main() -> None:
    work_dir = get_work_dir(create=False)
    src = source_dir(work_dir)
    mpath = manifest_path(work_dir)
    if not mpath.is_file():
        sys.exit(f"ERROR: {mpath} not found. Run generate_data.py first.")
    manifest = json.loads(mpath.read_text(encoding="utf-8"))

    # Verify every file matches the manifest before touching the database.
    for entry in manifest["files"]:
        path = src / entry["path"]
        if not path.is_file():
            sys.exit(f"ERROR: missing source file {path}")
        if sha256_file(path) != entry["sha256"]:
            sys.exit(f"ERROR: checksum mismatch for {path}")

    final_db = db_path(work_dir)
    tmp_db = final_db.with_name(final_db.name + ".tmp")
    tmp_db.unlink(missing_ok=True)

    conn = sqlite3.connect(tmp_db)
    try:
        conn.execute("PRAGMA foreign_keys = ON")
        conn.executescript(SCHEMA)
        with conn:
            for kind in ("policies", "claims"):
                entry = next(e for e in manifest["files"] if e["kind"] == kind)
                header, rows = read_rows(src / entry["path"])
                if len(rows) != entry["rows"]:
                    sys.exit(f"ERROR: {entry['path']} has {len(rows)} rows, manifest says {entry['rows']}")
                insert(conn, kind, header, rows)

            for entry in (e for e in manifest["files"] if e["kind"] == "payments"):
                header, rows = read_rows(src / entry["path"])
                if len(rows) != entry["rows"]:
                    sys.exit(f"ERROR: {entry['path']} has {len(rows)} rows, manifest says {entry['rows']}")
                batch_id = "BATCH-" + entry["batch_date"].replace("-", "")
                conn.execute(
                    "INSERT INTO load_batches (batch_id, batch_date, file_name, row_count, sha256) "
                    "VALUES (?, ?, ?, ?, ?)",
                    (batch_id, entry["batch_date"], entry["path"], len(rows), entry["sha256"]),
                )
                insert(conn, "payments", header, rows)

        fk_errors = conn.execute("PRAGMA foreign_key_check").fetchall()
        if fk_errors:
            sys.exit(f"ERROR: {len(fk_errors)} foreign key violations, e.g. {fk_errors[:5]}")
        integrity = conn.execute("PRAGMA integrity_check").fetchone()[0]
        if integrity != "ok":
            sys.exit(f"ERROR: integrity_check returned {integrity}")

        counts = {t: conn.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
                  for t in ("policies", "claims", "load_batches", "payments")}
        total = conn.execute("SELECT SUM(amount_paise) FROM payments").fetchone()[0]
    finally:
        conn.close()

    os.replace(tmp_db, final_db)
    print(f"Database      : {final_db}")
    for table, n in counts.items():
        print(f"{table:<14}: {n:,}")
    print(f"Payment total : {total:,} paise")


if __name__ == "__main__":
    main()
