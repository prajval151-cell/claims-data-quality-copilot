"""Shared helpers: resolve CLAIMS_WORK_DIR and the standard file layout inside it.

All generated data (CSVs, SQLite database, validation reports) lives under
CLAIMS_WORK_DIR, which must point outside this Git repository.
"""

from __future__ import annotations

import hashlib
import os
import sys
from pathlib import Path

ENV_VAR = "CLAIMS_WORK_DIR"
REPO_ROOT = Path(__file__).resolve().parents[1]


def _norm(path: Path) -> str:
    return os.path.normcase(os.path.normpath(str(path)))


def _is_within(child: Path, parent: Path) -> bool:
    c, p = _norm(child), _norm(parent)
    return c == p or c.startswith(p.rstrip(os.sep) + os.sep)


def get_work_dir(create: bool = True) -> Path:
    """Return the resolved CLAIMS_WORK_DIR, exiting if unset or inside the repo."""
    raw = os.environ.get(ENV_VAR, "").strip()
    if not raw:
        sys.exit(
            f"ERROR: {ENV_VAR} is not set. Point it at a directory outside the "
            f"repository, e.g. $env:{ENV_VAR} = \"$HOME\\claims-copilot-work\""
        )
    work_dir = Path(raw).expanduser().resolve()
    if _is_within(work_dir, REPO_ROOT):
        sys.exit(
            f"ERROR: {ENV_VAR} ({work_dir}) is inside the Git repository "
            f"({REPO_ROOT}). Choose a directory outside the repository."
        )
    if create:
        work_dir.mkdir(parents=True, exist_ok=True)
    elif not work_dir.is_dir():
        sys.exit(f"ERROR: {ENV_VAR} ({work_dir}) does not exist. Run generate_data.py first.")
    return work_dir


def source_dir(work_dir: Path) -> Path:
    return work_dir / "source"


def payments_dir(work_dir: Path) -> Path:
    return source_dir(work_dir) / "payments"


def manifest_path(work_dir: Path) -> Path:
    return source_dir(work_dir) / "manifest.json"


def db_path(work_dir: Path) -> Path:
    return work_dir / "claims.db"


def ensure_outside_repo(path: Path, label: str) -> Path:
    """Resolve path and exit if it lies inside the Git repository."""
    resolved = Path(path).expanduser().resolve()
    if _is_within(resolved, REPO_ROOT):
        sys.exit(f"ERROR: {label} ({resolved}) is inside the Git repository ({REPO_ROOT}).")
    return resolved


def sqlite_readonly(work_dir: Path, path: Path | None = None):
    """Open a SQLite source read-only (default: the Milestone 1 claims.db)."""
    import sqlite3

    path = ensure_outside_repo(path, "SQLite source") if path else db_path(work_dir)
    if not path.is_file():
        sys.exit(f"ERROR: {path} not found. Complete Milestone 1 first.")
    return sqlite3.connect(f"{path.as_uri()}?mode=ro", uri=True)


def warehouse_dir(work_dir: Path) -> Path:
    return work_dir / "warehouse"


def duckdb_path(work_dir: Path) -> Path:
    return warehouse_dir(work_dir) / "claims.duckdb"


def reports_dir(work_dir: Path) -> Path:
    return work_dir / "reports"


def incidents_dir(work_dir: Path) -> Path:
    return work_dir / "incidents"


def simulations_dir(work_dir: Path) -> Path:
    return work_dir / "simulations"


def duckdb_connect(path: Path, work_dir: Path, read_only: bool = False):
    """Open DuckDB with its temp, home, and extension dirs kept inside CLAIMS_WORK_DIR."""
    import duckdb

    path = ensure_outside_repo(path, "DuckDB database")
    if read_only and not path.is_file():
        sys.exit(f"ERROR: {path} not found. Run load_duckdb.py first.")
    path.parent.mkdir(parents=True, exist_ok=True)
    runtime = work_dir / "env" / "duckdb-runtime"
    (runtime / "tmp").mkdir(parents=True, exist_ok=True)
    return duckdb.connect(str(path), read_only=read_only, config={
        "temp_directory": str(runtime / "tmp"),
        "home_directory": str(runtime),
        "extension_directory": str(runtime / "extensions"),
        "autoinstall_known_extensions": False,
        "autoload_known_extensions": False,
    })


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()
