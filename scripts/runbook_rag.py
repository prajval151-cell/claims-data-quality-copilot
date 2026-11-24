"""Runbook retrieval (RAG retrieval step) for Claims Data Quality Copilot.

Usage:
  python scripts/runbook_rag.py build  [--runbooks DIR]
  python scripts/runbook_rag.py query  --incident INCIDENT.json [--top-k 3] [--runbooks DIR] [--output FILE]
  python scripts/runbook_rag.py verify [--incidents DIR] [--runbooks DIR]

build   Split runbooks/*.md into passages (one per section: the text before the first
        "## " heading is "Overview", then one per "## " heading). Embed each passage
        with a local sentence-transformers model on CPU and store the vector with the
        source filename, runbook title, section title, and passage text in
        $CLAIMS_WORK_DIR\\rag\\runbook_index.json.
query   For each finding in an incident JSON (from detect_anomalies.py), build a query
        from the finding type and its evidence, embed it, and rank passages by cosine
        similarity. Incidents with no findings skip retrieval. The report goes to
        $CLAIMS_WORK_DIR\\reports\\milestone4.
verify  Run query over every Milestone 3 incident and check that each finding's top
        passage comes from the runbook for that finding type and that the healthy
        incident skips retrieval.

Model: sentence-transformers/all-MiniLM-L6-v2 (384 dimensions, CPU). The model cache,
Hugging Face/torch caches, and temp files are kept under $CLAIMS_WORK_DIR\\env.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import re
import sys
from datetime import datetime
from pathlib import Path

from claims_workdir import REPO_ROOT, ensure_outside_repo, get_work_dir, incidents_dir, reports_dir

MODEL_NAME = "sentence-transformers/all-MiniLM-L6-v2"
DEFAULT_RUNBOOKS = REPO_ROOT / "runbooks"
EXPECTED_RUNBOOK = {
    "zero_row_load": "zero_row_load.md",
    "row_count_mismatch": "partial_load.md",
    "amount_mismatch": "amount_mismatch.md",
    "missing_expected_batch": "missing_or_stale_batches.md",
    "stale_data": "missing_or_stale_batches.md",
    "unusual_payment_total": "unusual_payment_totals.md",
}


# ---------------------------------------------------------------- environment / model

def configure_env(work_dir: Path) -> Path:
    """Point every model, cache, and temp location at CLAIMS_WORK_DIR before importing torch."""
    env = work_dir / "env"
    paths = {
        "HF_HOME": env / "hf-home",
        "SENTENCE_TRANSFORMERS_HOME": env / "hf-home" / "sentence-transformers",
        "TORCH_HOME": env / "torch-home",
        "XDG_CACHE_HOME": env / "xdg-cache",
        "TMP": env / "tmp",
        "TEMP": env / "tmp",
    }
    for key, path in paths.items():
        path.mkdir(parents=True, exist_ok=True)
        os.environ[key] = str(path)
    os.environ.update({"HF_HUB_DISABLE_TELEMETRY": "1", "HF_HUB_DISABLE_SYMLINKS_WARNING": "1",
                       "HF_HUB_DISABLE_PROGRESS_BARS": "1", "TRANSFORMERS_VERBOSITY": "error",
                       "TOKENIZERS_PARALLELISM": "false"})
    import tempfile
    tempfile.tempdir = str(paths["TMP"])
    return paths["SENTENCE_TRANSFORMERS_HOME"]


def load_model(work_dir: Path):
    cache = configure_env(work_dir)
    from sentence_transformers import SentenceTransformer

    try:  # use the cached copy without contacting the Hub
        return SentenceTransformer(MODEL_NAME, device="cpu", cache_folder=str(cache), local_files_only=True)
    except Exception:
        return SentenceTransformer(MODEL_NAME, device="cpu", cache_folder=str(cache))


def embed(model, texts: list[str]) -> list[list[float]]:
    return model.encode(texts, normalize_embeddings=True, convert_to_numpy=True).astype(float).tolist()


def cosine(a: list[float], b: list[float]) -> float:
    dot = sum(x * y for x, y in zip(a, b))
    return dot / (math.sqrt(sum(x * x for x in a)) * math.sqrt(sum(y * y for y in b)))


# ---------------------------------------------------------------- runbooks / index

def split_runbook(path: Path) -> list[dict]:
    text = path.read_text(encoding="utf-8")
    title_match = re.search(r"^# (.+)$", text, flags=re.M)
    title = title_match.group(1).strip() if title_match else path.stem
    body = text[title_match.end():] if title_match else text
    parts = re.split(r"^## (.+)$", body, flags=re.M)
    sections = [("Overview", parts[0])] + list(zip(parts[1::2], parts[2::2]))
    return [{"source_file": path.name, "runbook_title": title, "section_title": s.strip(),
             "text": t.strip()} for s, t in sections if t.strip()]


def runbooks_digest(runbooks: Path) -> str:
    h = hashlib.sha256()
    for p in sorted(runbooks.glob("*.md")):
        h.update(p.name.encode() + b"\0" + p.read_bytes() + b"\0")
    return h.hexdigest()


def index_path(work_dir: Path) -> Path:
    return work_dir / "rag" / "runbook_index.json"


def build(work_dir: Path, runbooks: Path) -> dict:
    files = sorted(runbooks.glob("*.md"))
    if not files:
        sys.exit(f"ERROR: no runbooks found in {runbooks}")
    passages = [p for f in files for p in split_runbook(f)]
    model = load_model(work_dir)
    vectors = embed(model, [f"{p['runbook_title']} - {p['section_title']}: {p['text']}" for p in passages])
    for i, (p, v) in enumerate(zip(passages, vectors), start=1):
        p["passage_id"] = f"P{i:03d}"
        p["vector"] = v
    index = {
        "model": MODEL_NAME,
        "dimensions": len(vectors[0]),
        "similarity": "cosine",
        "built_at": datetime.now().isoformat(timespec="seconds"),
        "runbooks_dir": str(runbooks),
        "runbooks_sha256": runbooks_digest(runbooks),
        "passages": [{k: p[k] for k in ("passage_id", "source_file", "runbook_title", "section_title",
                                         "text", "vector")} for p in passages],
    }
    out = ensure_outside_repo(index_path(work_dir), "vector index")
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(index) + "\n", encoding="utf-8")
    print(f"Indexed {len(passages)} passages from {len(files)} runbooks "
          f"({index['dimensions']} dims, {MODEL_NAME}) -> {out}")
    return index


def load_index(work_dir: Path, runbooks: Path) -> dict:
    path = index_path(work_dir)
    if not path.is_file():
        sys.exit(f"ERROR: {path} not found. Run: runbook_rag.py build")
    index = json.loads(path.read_text(encoding="utf-8"))
    if index["runbooks_sha256"] != runbooks_digest(runbooks):
        sys.exit("ERROR: runbooks changed since the index was built. Run: runbook_rag.py build")
    return index


# ---------------------------------------------------------------- query

def humanise(key: str) -> str:
    return key.replace("_", " ")


def build_query(f: dict) -> str:
    """Query text from the finding type and its evidence."""
    evidence = "; ".join(f"{humanise(k)} {v}" for k, v in f.get("evidence", {}).items() if v is not None)
    return (f"{humanise(f['check'])} ({humanise(f['category']).lower()}, {f['severity'].lower()} severity). "
            f"{f['message']}. Expected {f['expected']} {f['unit']}, observed {f['observed']} {f['unit']}. "
            f"Evidence: {evidence}.")


def retrieve(model, index: dict, incident: dict, top_k: int) -> dict:
    base = {"incident_id": incident.get("incident_id"), "label": incident.get("label"),
            "status": incident.get("status"), "model": index["model"], "similarity": "cosine"}
    findings = incident.get("findings") or []
    if not findings:
        return {**base, "retrieval": "SKIPPED",
                "reason": "incident has no findings (healthy run); no runbook retrieval needed",
                "results": []}
    queries = [build_query(f) for f in findings]
    q_vectors = embed(model, queries)
    results = []
    for f, q, qv in zip(findings, queries, q_vectors):
        scored = sorted(((cosine(qv, p["vector"]), p) for p in index["passages"]),
                        key=lambda sp: sp[0], reverse=True)[:top_k]
        results.append({
            "finding_id": f["finding_id"], "check": f["check"], "batch_id": f["batch_id"],
            "severity": f["severity"], "query": q,
            "passages": [{"rank": r, "score": round(s, 4), "passage_id": p["passage_id"],
                          "source_file": p["source_file"], "section_title": p["section_title"],
                          "runbook_title": p["runbook_title"], "text": p["text"]}
                         for r, (s, p) in enumerate(scored, start=1)],
        })
    return {**base, "retrieval": "EMBEDDING", "top_k": top_k, "results": results}


def report_path(work_dir: Path, incident_file: Path) -> Path:
    return reports_dir(work_dir) / "milestone4" / f"{incident_file.stem}_retrieval.json"


def write_json(path: Path, data: dict) -> Path:
    path = ensure_outside_repo(path, "report")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2) + "\n", encoding="utf-8")
    return path


def print_retrieval(rep: dict) -> None:
    print(f"Incident {rep['incident_id'] or '-'} ({rep['label']}): retrieval {rep['retrieval']}")
    if rep["retrieval"] == "SKIPPED":
        print(f"  {rep['reason']}")
    for r in rep["results"]:
        top = r["passages"][0]
        print(f"  {r['finding_id']} {r['check']:<22} {r['batch_id']}  -> {top['source_file']} "
              f"§ {top['section_title']}  (cosine {top['score']:.4f})")


# ---------------------------------------------------------------- verify

def verify(work_dir: Path, runbooks: Path, incidents: Path, top_k: int) -> bool:
    index = load_index(work_dir, runbooks)
    model = load_model(work_dir)
    files = sorted(p for p in incidents.glob("*.json") if p.name != "validation_summary.json")
    if not files:
        sys.exit(f"ERROR: no incident JSON files in {incidents}")
    rows, ok_all = [], True
    for path in files:
        incident = json.loads(path.read_text(encoding="utf-8"))
        rep = retrieve(model, index, incident, top_k)
        write_json(report_path(work_dir, path), rep)
        if rep["retrieval"] == "SKIPPED":
            ok = incident.get("status") == "NO_INCIDENT"
            ok_all &= ok
            rows.append({"incident": path.name, "finding_id": None, "check": None, "batch_id": None,
                         "top_file": None, "top_section": None, "score": None, "margin": None,
                         "expected_file": None, "retrieval": "SKIPPED", "passed": ok})
            continue
        for r in rep["results"]:
            top, expected = r["passages"][0], EXPECTED_RUNBOOK.get(r["check"])
            other = next((p for p in r["passages"] if p["source_file"] != top["source_file"]), None)
            ok = top["source_file"] == expected
            ok_all &= ok
            rows.append({"incident": path.name, "finding_id": r["finding_id"], "check": r["check"],
                         "batch_id": r["batch_id"], "top_file": top["source_file"],
                         "top_section": top["section_title"], "score": top["score"],
                         "margin": round(top["score"] - other["score"], 4) if other else None,
                         "expected_file": expected, "retrieval": "EMBEDDING", "passed": ok})

    def top_for(incident_name: str, check: str):
        return next((r for r in rows if r["incident"] == incident_name and r["check"] == check), None)

    zero, spike = top_for("zero_row_load.json", "zero_row_load"), top_for("payment_spike.json", "unusual_payment_total")
    healthy = next((r for r in rows if r["incident"] == "healthy.json"), None)
    required = {
        "zero_row_load incident -> zero_row_load.md": bool(zero and zero["top_file"] == "zero_row_load.md"),
        "payment_spike incident -> unusual_payment_totals.md":
            bool(spike and spike["top_file"] == "unusual_payment_totals.md"),
        "healthy incident skips retrieval": bool(healthy and healthy["retrieval"] == "SKIPPED"),
        "every finding's top passage is from its runbook": ok_all,
    }
    passed = all(required.values())
    summary = {"model": index["model"], "passages_indexed": len(index["passages"]), "top_k": top_k,
               "required_checks": required, "rows": rows, "passed": passed}
    out = write_json(reports_dir(work_dir) / "milestone4" / "verification_summary.json", summary)

    print(f"Index: {len(index['passages'])} passages, model {index['model']}, cosine similarity")
    for r in rows:
        if r["retrieval"] == "SKIPPED":
            print(f"[{'PASS' if r['passed'] else 'FAIL'}] {r['incident']:<22} retrieval SKIPPED (no findings)")
        else:
            print(f"[{'PASS' if r['passed'] else 'FAIL'}] {r['incident']:<22} {r['check']:<22} -> "
                  f"{r['top_file']} § {r['top_section']}  cosine {r['score']:.4f}  margin {r['margin']}")
    for name, ok in required.items():
        print(f"  [{'PASS' if ok else 'FAIL'}] {name}")
    print(f"RESULT: {'PASSED' if passed else 'FAILED'}")
    print(f"Reports: {out.parent}")
    return passed


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = parser.add_subparsers(dest="cmd", required=True)
    for name in ("build", "query", "verify"):
        p = sub.add_parser(name)
        p.add_argument("--runbooks", type=Path, default=DEFAULT_RUNBOOKS)
        if name in ("query", "verify"):
            p.add_argument("--top-k", type=int, default=3)
        if name == "query":
            p.add_argument("--incident", type=Path, required=True)
            p.add_argument("--output", type=Path)
        if name == "verify":
            p.add_argument("--incidents", type=Path)
    args = parser.parse_args()
    work_dir = get_work_dir(create=False)

    if args.cmd == "build":
        build(work_dir, args.runbooks)
    elif args.cmd == "query":
        index = load_index(work_dir, args.runbooks)
        incident = json.loads(args.incident.read_text(encoding="utf-8"))
        model = None if not incident.get("findings") else load_model(work_dir)
        rep = retrieve(model, index, incident, args.top_k)
        out = write_json(args.output or report_path(work_dir, args.incident), rep)
        print_retrieval(rep)
        print(f"Report: {out}")
    else:
        incidents = args.incidents or incidents_dir(work_dir) / "milestone3"
        sys.exit(0 if verify(work_dir, args.runbooks, incidents, args.top_k) else 1)


if __name__ == "__main__":
    main()
