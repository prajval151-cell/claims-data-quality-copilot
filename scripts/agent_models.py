"""Model backends for investigate_incident.py: a deterministic mock and the live Claude API.

Both expose respond(system, messages, tools) -> ModelTurn, where ModelTurn.blocks are plain
dicts ({"type": "text"|"tool_use", ...}) and ModelTurn.history_content is what the agent
appends to the conversation as the assistant turn (for Claude: response.content unchanged,
so thinking blocks are passed back as the API requires).

MockModel  - no network, no credentials. It plays the model's role in the same tool loop:
             turn 1 requests MCP evidence, turn 2 submits a report whose facts are read
             from the MCP tool results it received and whose causes and next step come
             from the retrieved runbook sections. It is a test double for the workflow,
             not an analysis.
ClaudeModel - Messages API via the official anthropic SDK; reads ANTHROPIC_API_KEY from the
             environment. Any API error, refusal, or truncation raises ModelError so the
             agent can fall back to the original SQL findings.
"""

from __future__ import annotations

import json
import os
import re
from dataclasses import dataclass, field
from typing import Any

LIVE_MODEL = "claude-opus-5-5"
LIVE_EFFORT = "high"
LIVE_MAX_TOKENS = 16000
MOCK_MODEL = "mock-investigator-v1"


class ModelError(Exception):
    """The model call failed or produced unusable output."""


class ModelUnavailable(ModelError):
    """The live model cannot be used (no credentials)."""


@dataclass
class ModelTurn:
    blocks: list[dict]
    stop_reason: str
    history_content: Any
    request_id: str | None = None
    usage: dict = field(default_factory=dict)
    served_by: str | None = None


def fmt(n: Any) -> str:
    return f"{n:,}" if isinstance(n, int) and not isinstance(n, bool) else str(n)


# ---------------------------------------------------------------- mock

class MockModel:
    mode = "mock"
    model_id = MOCK_MODEL

    def __init__(self, fail: bool = False):
        self.fail = fail
        self.context: dict = {}
        self._ids = 0

    def bind(self, incident: dict, retrieval: dict) -> None:
        self.context = {"incident": incident, "retrieval": retrieval}

    def _id(self) -> str:
        self._ids += 1
        return f"toolu_mock_{self._ids:03d}"

    def respond(self, system: str, messages: list[dict], tools: list[dict]) -> ModelTurn:
        if self.fail:
            raise ModelError("simulated model failure (--simulate-model-failure)")
        names = {t["name"] for t in tools}
        results = _collect_tool_results(messages)
        if not results:  # turn 1: ask for evidence
            blocks = [{"type": "text", "text": "Requesting run and batch evidence."}]
            blocks += [{"type": "tool_use", "id": self._id(), "name": n, "input": a}
                       for n, a in self._evidence_plan(names)]
        else:  # turn 2: submit the report built from tool results and runbook sections
            report = self._compose(results)
            blocks = [{"type": "tool_use", "id": self._id(), "name": "submit_report", "input": report}]
        return ModelTurn(blocks=blocks, stop_reason="tool_use", history_content=blocks)

    def _evidence_plan(self, names: set[str]) -> list[tuple[str, dict]]:
        plan: list[tuple[str, dict]] = [("get_run_summary", {})]
        for f in self.context["incident"]["findings"]:
            b = f.get("batch_id")
            if not b:
                continue
            plan.append(("get_batch_audit", {"batch_id": b}))
            if f["check"] == "unusual_payment_total":
                plan += [("get_batch_profile", {"batch_id": b, "layer": "source"}),
                         ("get_batch_profile", {"batch_id": b, "layer": "target"}),
                         ("get_batch_history", {"batch_id": b, "window": 7})]
        seen, out = set(), []
        for n, a in plan:
            key = (n, json.dumps(a, sort_keys=True))
            if n in names and key not in seen:
                seen.add(key)
                out.append((n, a))
        return out

    def _compose(self, results: list[dict]) -> dict:
        ok = [r for r in results if not r.get("is_error")]
        summary_ev = next((r for r in ok if r["tool"] == "get_run_summary"), None)
        findings_out = []
        for f in self.context["incident"]["findings"]:
            fid, b = f["finding_id"], f.get("batch_id")
            mine = [r for r in ok if r["arguments"].get("batch_id") == b]
            audit = next((r for r in mine if r["tool"] == "get_batch_audit"), None)
            hist = next((r for r in mine if r["tool"] == "get_batch_history"), None)
            prof = next((r for r in mine if r["tool"] == "get_batch_profile"
                         and r["arguments"].get("layer") == "source"), None)
            facts = []
            if audit:
                a = audit["result"]
                facts.append(_claim(
                    f"{b} in load run {a['scope']['load_run_id']}: source {fmt(a['source_rows'])} rows / "
                    f"{fmt(a['source_amount_paise'])} paise, staging {fmt(a['staging_rows'])} rows / "
                    f"{fmt(a['staging_amount_paise'])} paise, target {fmt(a['target_rows'])} rows / "
                    f"{fmt(a['target_amount_paise'])} paise.", [audit["evidence_id"]]))
                if a["reconciled"]:
                    facts.append(_claim(
                        f"Source and target reconciled for {b}: {fmt(a['source_rows'])} = "
                        f"{fmt(a['target_rows'])} rows and {fmt(a['source_amount_paise'])} = "
                        f"{fmt(a['target_amount_paise'])} paise, so no rows were lost or changed by the load.",
                        [audit["evidence_id"]]))
                else:
                    stage = a["first_divergence"].replace("_to_", " -> ")
                    facts.append(_claim(
                        f"The stages first diverge at {stage}: {fmt(a['rows_lost_source_to_staging'])} rows "
                        f"lost source -> staging, {fmt(a['rows_lost_staging_to_target'])} lost staging -> target.",
                        [audit["evidence_id"]]))
            if summary_ev:
                s = summary_ev["result"]
                lr, t = s["load_run"], s["totals"]
                facts.append(_claim(
                    f"Load run {lr['run_id']} status {lr['status']}; staging filter recorded: "
                    f"{lr['staging_filter']!r}. Run totals: source {fmt(t['source_rows'])}, staging "
                    f"{fmt(t['staging_rows'])}, target {fmt(t['target_rows'])} rows; "
                    f"{s['batches_mismatched']} of {fmt(t['batches'])} batches do not reconcile.",
                    [summary_ev["evidence_id"]]))
            if hist and hist["result"]["this_batch_target_vs_median_ratio"] is not None:
                h = hist["result"]
                facts.append(_claim(
                    f"{b} target total is {h['this_batch_target_vs_median_ratio']}x the median "
                    f"({fmt(h['prior_reconciled_median_target_paise'])} paise) of the "
                    f"{h['prior_reconciled_batches']} prior reconciled batches; row count "
                    f"{fmt(h['this_batch']['target_rows'])} vs prior median {fmt(h['this_batch_rows_vs_prior_median_rows'])}.",
                    [hist["evidence_id"]]))
            if prof:
                p = prof["result"]
                facts.append(_claim(
                    f"Source profile: median payment {fmt(p['amount_paise_distribution']['p50'])} paise; "
                    f"the top 1% of payments ({p['top_1pct_payments']['count']}) hold "
                    f"{p['top_1pct_payments']['share_of_total']:.1%} of the total.", [prof["evidence_id"]]))
            core = [e for e in (audit, summary_ev) if e]
            core_ids = [e["evidence_id"] for e in core] or [r["evidence_id"] for r in ok[:1]]
            causes_p, checks_p = self._passage(fid, "Possible causes"), self._passage(fid, "Checks to run")
            causes = [_claim(f"Possible cause (from runbook): {c}", core_ids, [causes_p["ref"]])
                      for c in _bullets(causes_p["text"])[:2]] if causes_p else []
            step = None
            if checks_p:
                reconciled = bool(audit and audit["result"]["reconciled"])
                staging_loss = bool(audit and audit["result"]["first_divergence"] == "source_to_staging")
                checks = _bullets(checks_p["text"])
                if reconciled:
                    checks = [c for c in checks if "reconciliation" not in c.lower()] or checks
                if staging_loss:
                    checks = sorted(checks, key=lambda c: "staging" not in c.lower())
                step = _claim(checks[0], core_ids, [checks_p["ref"]])
            findings_out.append({"finding_id": fid, "observed_facts": facts, "possible_causes": causes,
                                 "next_verification_step": step or _claim("", [], [])})
        inc = self.context["incident"]
        return {"summary": f"Mock investigation of {inc.get('label')} ({inc.get('incident_id')}): "
                           f"{len(findings_out)} finding(s) checked against MCP evidence and runbooks.",
                "findings": findings_out}

    def _passage(self, finding_id: str, section: str) -> dict | None:
        for r in self.context["retrieval"]["results"]:
            if r["finding_id"] == finding_id:
                return next((p for p in r["passages"] if p["section_title"] == section), None)
        return None


def _claim(text: str, evidence_ids: list[str], runbook_refs: list[str] | None = None) -> dict:
    return {"text": text, "evidence_ids": list(evidence_ids), "runbook_refs": list(runbook_refs or [])}


def _bullets(text: str) -> list[str]:
    return [m.strip() for m in re.findall(r"^\s*-\s+(.+)$", text, flags=re.M)]


def _collect_tool_results(messages: list[dict]) -> list[dict]:
    """Parse the agent's JSON tool_result payloads from the conversation."""
    out = []
    for m in messages:
        if m["role"] != "user" or not isinstance(m["content"], list):
            continue
        for block in m["content"]:
            if isinstance(block, dict) and block.get("type") == "tool_result":
                try:
                    payload = json.loads(block["content"])
                except (TypeError, ValueError):
                    continue
                if "evidence_id" in payload:
                    out.append(payload)
    return out


# ---------------------------------------------------------------- live Claude

def live_key_present() -> bool:
    return bool(os.environ.get("ANTHROPIC_API_KEY", "").strip())


class ClaudeModel:
    mode = "live"
    model_id = LIVE_MODEL

    def __init__(self, model: str = LIVE_MODEL, effort: str = LIVE_EFFORT):
        if not live_key_present():
            raise ModelUnavailable("ANTHROPIC_API_KEY is not set")
        import anthropic

        self.anthropic = anthropic
        # Key comes only from the environment; it is never logged or written to disk.
        self.client = anthropic.Anthropic(api_key=os.environ["ANTHROPIC_API_KEY"].strip(),
                                          max_retries=2, timeout=600.0)
        self.model_id, self.effort = model, effort

    def bind(self, incident: dict, retrieval: dict) -> None:
        pass

    def respond(self, system: str, messages: list[dict], tools: list[dict]) -> ModelTurn:
        a = self.anthropic
        try:
            resp = self.client.beta.messages.create(
                model=self.model_id,
                max_tokens=LIVE_MAX_TOKENS,
                system=system,
                messages=messages,
                tools=tools,
                tool_choice={"type": "auto"},
                output_config={"effort": self.effort},
                # Server-side refusal fallback: on a policy decline the API retries on a
                # substitute model chosen by refusal category.
                betas=["server-side-fallback-2026-07-01"],
                fallbacks="default",
            )
        except a.AuthenticationError as e:
            raise ModelError(f"authentication failed (check ANTHROPIC_API_KEY): {e.message}") from e
        except a.PermissionDeniedError as e:
            raise ModelError(f"permission denied: {e.message}") from e
        except a.NotFoundError as e:
            raise ModelError(f"model or endpoint not found: {e.message}") from e
        except a.RateLimitError as e:
            raise ModelError(f"rate limited after retries: {e.message}") from e
        except a.BadRequestError as e:
            raise ModelError(f"bad request: {e.message}") from e
        except a.APIStatusError as e:
            raise ModelError(f"API error {e.status_code}: {e.message}") from e
        except a.APIConnectionError as e:
            raise ModelError(f"network error: {e}") from e

        if resp.stop_reason == "refusal":
            details = getattr(resp, "stop_details", None)
            raise ModelError(f"model refused (category {getattr(details, 'category', None)})")
        if resp.stop_reason == "max_tokens":
            raise ModelError(f"response truncated at max_tokens={LIVE_MAX_TOKENS}")

        blocks = []
        for b in resp.content:
            if b.type == "text":
                blocks.append({"type": "text", "text": b.text})
            elif b.type == "tool_use":
                blocks.append({"type": "tool_use", "id": b.id, "name": b.name, "input": b.input})
        u = resp.usage
        usage = {"input_tokens": u.input_tokens, "output_tokens": u.output_tokens}
        return ModelTurn(blocks=blocks, stop_reason=resp.stop_reason, history_content=resp.content,
                         request_id=getattr(resp, "_request_id", None), usage=usage, served_by=resp.model)
