"""Read-only MCP server: aggregate claim-payment evidence for ONE load run.

Usage (normally launched over stdio by investigate_incident.py):
  python scripts/claims_mcp_server.py --incident INCIDENT.json

Scope is fixed at startup from the Milestone 3 incident JSON and cannot be changed by a
tool call:
  * DuckDB file   - incident["sources"]["duckdb"]; for simulated incidents this is the
                    simulation warehouse that produced the anomaly, so the agent sees the
                    same stage counts that triggered it
  * SQLite source - incident["sources"]["sqlite"] (a path for simulated sources such as
                    the payment-spike copy, otherwise the Milestone 1 claims.db)
  * load run      - incident["sources"]["latest_load_run"]["run_id"]

Both databases must be under CLAIMS_WORK_DIR and outside the Git repository. They are
opened read-only (DuckDB read_only=True, SQLite mode=ro) for every call. Every query is
parameterized; table and column names are constants. batch_id must match BATCH-YYYYMMDD
and belong to the selected run. Tools return aggregates only (counts, paise totals,
percentiles, group-by totals); no payment, claim, or transaction identifiers.

Tools:
  get_run_summary()                   load run record and run-level stage totals
  get_batch_audit(batch_id)           source / staging / target rows and paise for one batch
  get_batch_profile(batch_id, layer)  amount distribution and breakdowns, source or target
  get_batch_history(batch_id, window) prior batches' totals in this run, with baseline median
"""

from __future__ import annotations

import argparse
import json
import os
import re
import statistics
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal

from claims_workdir import (
    _is_within,
    db_path,
    duckdb_connect,
    ensure_outside_repo,
    get_work_dir,
    sqlite_readonly,
)

BATCH_RE = re.compile(r"^BATCH-\d{8}$")
MAX_HISTORY_WINDOW = 28
MISMATCH_LIST_LIMIT = 20
MILESTONE1_SOURCE_LABEL = "claims.db (Milestone 1)"
READ_ONLY = {"read_only_hint": True, "destructive_hint": False, "idempotent_hint": True,
             "open_world_hint": False}

# Fixed table/column names: never built from tool input.
PROFILE_TABLES = {
    # layer -> (table, extra WHERE clause binding the run, needs run_id parameter)
    "source": ("payments", "", False),
    "target": ("fact_claim_payment", " AND load_run_id = ?", True),
}
MATCH_SQL = ("source_rows = staging_rows AND staging_rows = target_rows "
             "AND source_amount_paise = staging_amount_paise "
             "AND staging_amount_paise = target_amount_paise")


class ScopeError(ValueError):
    """An expected rejection (bad or out-of-scope input); its message is safe to show the model."""


@dataclass(frozen=True)
class Scope:
    work_dir: Path
    incident_file: Path
    incident_label: str | None
    duckdb: Path
    sqlite: Path
    source_label: str
    run_id: int

    def describe(self) -> dict:
        return {"incident": self.incident_label, "load_run_id": self.run_id,
                "duckdb_file": self.duckdb.name, "source": self.source_label}


def _inside_work_dir(path: Path, work_dir: Path, label: str) -> Path:
    resolved = ensure_outside_repo(path, label)
    if not _is_within(resolved, work_dir):
        sys.exit(f"ERROR: {label} ({resolved}) is outside CLAIMS_WORK_DIR ({work_dir}).")
    if not resolved.is_file():
        sys.exit(f"ERROR: {label} ({resolved}) not found.")
    return resolved


def resolve_scope(incident_file: Path, work_dir: Path) -> Scope:
    incident = json.loads(Path(incident_file).read_text(encoding="utf-8"))
    sources = incident["sources"]
    duck = _inside_work_dir(Path(sources["duckdb"]), work_dir, "incident DuckDB")
    raw_sqlite = sources.get("sqlite") or MILESTONE1_SOURCE_LABEL
    if raw_sqlite == MILESTONE1_SOURCE_LABEL:
        sqlite, label = _inside_work_dir(db_path(work_dir), work_dir, "SQLite source"), MILESTONE1_SOURCE_LABEL
    else:
        sqlite = _inside_work_dir(Path(raw_sqlite), work_dir, "incident SQLite source")
        label = f"simulated source {sqlite.name}"
    run_id = int(sources["latest_load_run"]["run_id"])
    scope = Scope(work_dir, Path(incident_file).resolve(), incident.get("label"), duck, sqlite, label, run_id)
    with _duck(scope) as con:
        if con.execute("SELECT COUNT(*) FROM etl_load_run WHERE run_id = ?", [run_id]).fetchone()[0] != 1:
            sys.exit(f"ERROR: load run {run_id} not found in {duck}")
    return scope


# ---------------------------------------------------------------- connections

class _duck:
    """Read-only DuckDB connection as a context manager."""

    def __init__(self, scope: Scope):
        self.scope = scope

    def __enter__(self):
        self.con = duckdb_connect(self.scope.duckdb, self.scope.work_dir, read_only=True)
        return self.con

    def __exit__(self, *exc):
        self.con.close()


class _sqlite:
    """Read-only SQLite connection (mode=ro) as a context manager."""

    def __init__(self, scope: Scope):
        self.scope = scope

    def __enter__(self):
        self.con = sqlite_readonly(self.scope.work_dir, self.scope.sqlite)
        return self.con

    def __exit__(self, *exc):
        self.con.close()


# ---------------------------------------------------------------- validation

def check_batch_id(batch_id: str) -> str:
    if not isinstance(batch_id, str) or not BATCH_RE.fullmatch(batch_id):
        raise ScopeError("batch_id must look like BATCH-YYYYMMDD")
    return batch_id


def require_batch_in_run(con, scope: Scope, batch_id: str) -> None:
    found = con.execute("SELECT COUNT(*) FROM etl_batch_audit WHERE run_id = ? AND batch_id = ?",
                        [scope.run_id, batch_id]).fetchone()[0]
    if not found:
        raise ScopeError(f"{batch_id} has no audit record in load run {scope.run_id}")


def _iso(v: Any) -> Any:
    return v.isoformat() if hasattr(v, "isoformat") else v


def _int(v: Any) -> int | None:
    return None if v is None else int(v)


# ---------------------------------------------------------------- tool logic (plain functions)

def run_summary(scope: Scope) -> dict:
    with _duck(scope) as con:
        run = con.execute("SELECT run_id, status, started_at, finished_at, staging_filter, source_db "
                          "FROM etl_load_run WHERE run_id = ?", [scope.run_id]).fetchone()
        t = con.execute(
            "SELECT COUNT(*), SUM(source_rows), SUM(staging_rows), SUM(target_rows), "
            "SUM(source_amount_paise), SUM(staging_amount_paise), SUM(target_amount_paise), "
            f"SUM(CASE WHEN {MATCH_SQL} THEN 1 ELSE 0 END) "
            "FROM etl_batch_audit WHERE run_id = ?", [scope.run_id]).fetchone()
        mismatched = con.execute(
            "SELECT batch_id, source_rows, staging_rows, target_rows, source_amount_paise, "
            "staging_amount_paise, target_amount_paise FROM etl_batch_audit "
            f"WHERE run_id = ? AND NOT ({MATCH_SQL}) ORDER BY batch_date LIMIT ?",
            [scope.run_id, MISMATCH_LIST_LIMIT]).fetchall()
    totals = dict(zip(["batches", "source_rows", "staging_rows", "target_rows", "source_amount_paise",
                       "staging_amount_paise", "target_amount_paise"], map(_int, t[:7])))
    totals["row_diff_target_minus_source"] = totals["target_rows"] - totals["source_rows"]
    totals["amount_diff_target_minus_source_paise"] = totals["target_amount_paise"] - totals["source_amount_paise"]
    keys = ["batch_id", "source_rows", "staging_rows", "target_rows", "source_amount_paise",
            "staging_amount_paise", "target_amount_paise"]
    return {
        "scope": scope.describe(),
        "load_run": {"run_id": run[0], "status": run[1], "started_at": _iso(run[2]),
                     "finished_at": _iso(run[3]), "staging_filter": run[4],
                     "source_db_file": Path(run[5]).name if run[5] else None},
        "totals": totals,
        "batches_reconciled": _int(t[7]),
        "batches_mismatched": totals["batches"] - _int(t[7]),
        "run_reconciled": totals["batches"] == _int(t[7]),
        "mismatched_batches": [dict(zip(keys, map(lambda v: v if isinstance(v, str) else _int(v), r)))
                               for r in mismatched],
        "mismatched_batches_truncated_at": MISMATCH_LIST_LIMIT,
    }


def batch_audit(scope: Scope, batch_id: str) -> dict:
    check_batch_id(batch_id)
    with _duck(scope) as con:
        require_batch_in_run(con, scope, batch_id)
        r = con.execute(
            "SELECT batch_date, source_rows, staging_rows, target_rows, source_amount_paise, "
            "staging_amount_paise, target_amount_paise FROM etl_batch_audit "
            "WHERE run_id = ? AND batch_id = ?", [scope.run_id, batch_id]).fetchone()
        now_rows, now_paise = con.execute(
            "SELECT COUNT(*), COALESCE(SUM(amount_paise), 0) FROM fact_claim_payment "
            "WHERE batch_id = ? AND load_run_id = ?", [batch_id, scope.run_id]).fetchone()
    sr, gr, tr, sa, ga, ta = (int(v) for v in r[1:])
    if (sr, sa) != (gr, ga):
        divergence = "source_to_staging"
    elif (gr, ga) != (tr, ta):
        divergence = "staging_to_target"
    else:
        divergence = "none"
    return {
        "scope": scope.describe(),
        "batch_id": batch_id, "batch_date": _iso(r[0]),
        "source_rows": sr, "staging_rows": gr, "target_rows": tr,
        "source_amount_paise": sa, "staging_amount_paise": ga, "target_amount_paise": ta,
        "rows_lost_source_to_staging": sr - gr, "rows_lost_staging_to_target": gr - tr,
        "paise_diff_staging_minus_source": ga - sa, "paise_diff_target_minus_staging": ta - ga,
        "reconciled": divergence == "none",
        "first_divergence": divergence,
        "fact_table_now": {"rows": int(now_rows), "amount_paise": int(now_paise),
                           "matches_audit_target": (int(now_rows), int(now_paise)) == (tr, ta)},
    }


def _percentile(sorted_vals: list[int], q: float) -> int | None:
    if not sorted_vals:
        return None
    idx = min(len(sorted_vals) - 1, max(0, round(q * (len(sorted_vals) - 1))))
    return int(sorted_vals[idx])


def batch_profile(scope: Scope, batch_id: str, layer: str) -> dict:
    check_batch_id(batch_id)
    if layer not in PROFILE_TABLES:
        raise ScopeError("layer must be 'source' or 'target'")
    table, run_clause, needs_run = PROFILE_TABLES[layer]
    params = [batch_id] + ([scope.run_id] if needs_run else [])
    where = f"WHERE batch_id = ?{run_clause}"

    with _duck(scope) as duck:  # batch must belong to the selected run for either layer
        require_batch_in_run(duck, scope, batch_id)
    opener = _sqlite(scope) if layer == "source" else _duck(scope)
    with opener as con:
        head = con.execute(
            f"SELECT COUNT(*), COALESCE(SUM(amount_paise), 0), COUNT(DISTINCT claim_id), "
            f"MIN(payment_date), MAX(payment_date) FROM {table} {where}", params).fetchone()
        amounts = [int(a) for (a,) in con.execute(
            f"SELECT amount_paise FROM {table} {where} ORDER BY amount_paise", params).fetchall()]
        groups = {}
        for col in ("payment_method", "payee_type"):  # constant column names
            groups[col] = [{"value": v, "rows": int(n), "amount_paise": int(s)} for v, n, s in con.execute(
                f"SELECT {col}, COUNT(*), SUM(amount_paise) FROM {table} {where} GROUP BY {col} ORDER BY {col}",
                params).fetchall()]
    total = int(head[1])
    top_n = max(1, len(amounts) // 100) if amounts else 0
    top_share = round(sum(amounts[-top_n:]) / total, 4) if total else None
    return {
        "scope": scope.describe(), "batch_id": batch_id, "layer": layer,
        "table": table + (f" (load_run_id = {scope.run_id})" if needs_run else f" in {scope.sqlite.name}"),
        "rows": int(head[0]), "amount_paise": total, "distinct_claims": int(head[2]),
        "payment_date_min": _iso(head[3]), "payment_date_max": _iso(head[4]),
        "amount_paise_distribution": {
            "min": _percentile(amounts, 0.0), "p50": _percentile(amounts, 0.5),
            "p90": _percentile(amounts, 0.9), "p99": _percentile(amounts, 0.99),
            "max": _percentile(amounts, 1.0),
            "mean": round(total / len(amounts)) if amounts else None,
        },
        "top_1pct_payments": {"count": top_n, "share_of_total": top_share},
        "by_payment_method": groups["payment_method"],
        "by_payee_type": groups["payee_type"],
    }


def batch_history(scope: Scope, batch_id: str, window: int) -> dict:
    check_batch_id(batch_id)
    if not isinstance(window, int) or not 1 <= window <= MAX_HISTORY_WINDOW:
        raise ScopeError(f"window must be an integer from 1 to {MAX_HISTORY_WINDOW}")
    cols = ["batch_id", "batch_date", "source_rows", "staging_rows", "target_rows",
            "source_amount_paise", "staging_amount_paise", "target_amount_paise"]
    select = ", ".join(cols)
    with _duck(scope) as con:
        require_batch_in_run(con, scope, batch_id)
        this = con.execute(f"SELECT {select} FROM etl_batch_audit WHERE run_id = ? AND batch_id = ?",
                           [scope.run_id, batch_id]).fetchone()
        prior = con.execute(
            f"SELECT {select} FROM etl_batch_audit WHERE run_id = ? AND batch_date < ? "
            "ORDER BY batch_date DESC LIMIT ?", [scope.run_id, this[1], window]).fetchall()

    def row(r) -> dict:
        d = {c: (_iso(v) if c == "batch_date" else v if c == "batch_id" else int(v)) for c, v in zip(cols, r)}
        d["reconciled"] = (d["source_rows"] == d["staging_rows"] == d["target_rows"]
                           and d["source_amount_paise"] == d["staging_amount_paise"] == d["target_amount_paise"])
        return d

    prior_rows = [row(r) for r in reversed(prior)]
    clean = [p["target_amount_paise"] for p in prior_rows if p["reconciled"]]
    median = int(statistics.median(clean)) if clean else None
    current = row(this)
    return {
        "scope": scope.describe(), "batch_id": batch_id, "window_requested": window,
        "this_batch": current,
        "prior_batches": prior_rows,
        "prior_reconciled_batches": len(clean),
        "prior_reconciled_median_target_paise": median,
        "this_batch_target_vs_median_ratio": round(current["target_amount_paise"] / median, 4) if median else None,
        "this_batch_rows_vs_prior_median_rows": (
            int(statistics.median(p["target_rows"] for p in prior_rows)) if prior_rows else None),
    }


# ---------------------------------------------------------------- MCP server

def build_server(scope: Scope):
    from mcp.server.mcpserver import MCPServer
    from mcp.server.mcpserver.exceptions import ToolError
    from mcp_types import ToolAnnotations

    def guard(fn, *args):
        try:
            return fn(scope, *args)
        except ScopeError as exc:  # expected: report the reason to the client
            raise ToolError(str(exc)) from exc

    server = MCPServer(
        "claims-dq-evidence",
        instructions=(f"Read-only aggregate evidence for load run {scope.run_id} "
                      f"({scope.duckdb.name}, {scope.source_label}). Scope is fixed."),
        log_level="ERROR",
    )
    ro = ToolAnnotations(**READ_ONLY)

    @server.tool(annotations=ro)
    def get_run_summary() -> dict[str, Any]:
        """Load-run record (status, start/finish time, staging filter) and run-level totals of
        source, staging and target rows and paise from etl_batch_audit, plus the batches whose
        stages do not reconcile. Scoped to the incident's load run."""
        return guard(run_summary)

    @server.tool(annotations=ro)
    def get_batch_audit(batch_id: str) -> dict[str, Any]:
        """Stage counts for one batch in the incident's load run: source, staging and target rows
        and paise from etl_batch_audit, where they first diverge, and whether fact_claim_payment
        still matches the audited target. batch_id format: BATCH-YYYYMMDD."""
        return guard(batch_audit, batch_id)

    @server.tool(annotations=ro)
    def get_batch_profile(batch_id: str, layer: Literal["source", "target"]) -> dict[str, Any]:
        """Aggregate profile of one batch in the source database ('source') or in the target fact
        table for the incident's load run ('target'): row count, paise total, distinct claims,
        amount percentiles, share of the top 1% of payments, and totals by payment method and
        payee type. No row-level identifiers."""
        return guard(batch_profile, batch_id, layer)

    @server.tool(annotations=ro)
    def get_batch_history(batch_id: str, window: int = 7) -> dict[str, Any]:
        """Per-batch stage totals for up to `window` (1-28) batches before batch_id in the
        incident's load run, the median target paise of the prior reconciled batches, and this
        batch's ratio to that median."""
        return guard(batch_history, batch_id, window)

    return server


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--incident", type=Path, required=True, help="Milestone 3 incident JSON")
    parser.add_argument("--describe", action="store_true", help="print the resolved scope and exit")
    args = parser.parse_args()
    work_dir = get_work_dir(create=False)
    scope = resolve_scope(args.incident, work_dir)
    if args.describe:
        print(json.dumps({**scope.describe(), "duckdb": str(scope.duckdb), "sqlite": str(scope.sqlite)}, indent=2))
        return
    os.environ.pop("ANTHROPIC_API_KEY", None)  # the evidence server never needs model credentials
    build_server(scope).run()  # stdio transport


if __name__ == "__main__":
    main()
