"""Generate deterministic synthetic insurance source data.

Outputs (under $CLAIMS_WORK_DIR/source):
  policies.csv
  claims.csv
  payments/payments_YYYY-MM-DD.csv   (60 daily batches x 1,500 payments)
  manifest.json                      (seed, row counts, SHA-256 per file)

All monetary amounts are integer paise (1 rupee = 100 paise).
Uses only the Python standard library and a fixed seed, so repeated runs
produce byte-identical files.
"""

from __future__ import annotations

import csv
import json
import random
import string
from bisect import bisect_right
from datetime import date, timedelta
from pathlib import Path

from claims_workdir import (
    get_work_dir,
    manifest_path,
    payments_dir,
    sha256_file,
    source_dir,
)

SEED = 20260101
NUM_POLICIES = 6_000
NUM_CLAIMS = 24_000
NUM_BATCHES = 60
PAYMENTS_PER_BATCH = 1_500
TOTAL_PAYMENTS = NUM_BATCHES * PAYMENTS_PER_BATCH  # 90,000

BATCH_START = date(2026, 1, 1)
BATCH_END = BATCH_START + timedelta(days=NUM_BATCHES - 1)
CLAIM_WINDOW_START = BATCH_START - timedelta(days=90)
POLICY_START_MIN = date(2025, 1, 1)
POLICY_START_MAX = date(2025, 12, 31)
POLICY_TERM_DAYS = 365

PRODUCTS = {
    # product: (weight, sum insured options in rupees, premium rate per mille, claim types, payee types)
    "HEALTH": (45, [300_000, 500_000, 1_000_000, 2_500_000], 28,
               ["HOSPITALISATION", "DAY_CARE", "MATERNITY", "OPD"], ["HOSPITAL", "INSURED"]),
    "MOTOR": (35, [200_000, 400_000, 800_000, 1_500_000], 22,
              ["OWN_DAMAGE", "THEFT", "THIRD_PARTY"], ["GARAGE", "INSURED", "THIRD_PARTY"]),
    "HOME": (12, [1_000_000, 2_500_000, 5_000_000], 4,
             ["FIRE", "FLOOD", "BURGLARY"], ["INSURED", "VENDOR"]),
    "TRAVEL": (8, [100_000, 300_000, 500_000], 15,
               ["MEDICAL_EMERGENCY", "TRIP_CANCELLATION", "BAGGAGE_LOSS"], ["INSURED", "HOSPITAL"]),
}
STATES = ["MH", "KA", "TN", "DL", "GJ", "UP", "WB", "TG", "RJ", "KL"]
CLAIM_STATUSES = [("APPROVED", 80), ("REJECTED", 12), ("PENDING", 8)]
PAYMENT_METHODS = [("NEFT", 50), ("RTGS", 15), ("IMPS", 20), ("UPI", 10), ("CHEQUE", 5)]

POLICY_FIELDS = ["policy_id", "product_type", "state_code", "start_date", "end_date",
                 "sum_insured_paise", "annual_premium_paise"]
CLAIM_FIELDS = ["claim_id", "policy_id", "claim_date", "claim_type", "claim_status",
                "claimed_amount_paise", "approved_amount_paise"]
PAYMENT_FIELDS = ["payment_id", "batch_id", "claim_id", "payment_date", "amount_paise",
                  "payment_method", "payee_type", "transaction_ref"]


def weighted(rng: random.Random, options: list[tuple[str, int]]) -> str:
    return rng.choices([o for o, _ in options], weights=[w for _, w in options])[0]


def random_date(rng: random.Random, lo: date, hi: date) -> date:
    return lo + timedelta(days=rng.randint(0, (hi - lo).days))


def generate_policies(rng: random.Random) -> list[dict]:
    product_weights = [(p, cfg[0]) for p, cfg in PRODUCTS.items()]
    policies = []
    for i in range(1, NUM_POLICIES + 1):
        product = weighted(rng, product_weights)
        _, sums, rate, _, _ = PRODUCTS[product]
        sum_insured_paise = rng.choice(sums) * 100
        start = random_date(rng, POLICY_START_MIN, POLICY_START_MAX)
        policies.append({
            "policy_id": f"POL-{i:06d}",
            "product_type": product,
            "state_code": rng.choice(STATES),
            "start_date": start,
            "end_date": start + timedelta(days=POLICY_TERM_DAYS - 1),
            "sum_insured_paise": sum_insured_paise,
            "annual_premium_paise": sum_insured_paise * rate // 1000,
        })
    return policies


def generate_claims(rng: random.Random, policies: list[dict]) -> list[dict]:
    drafts = []
    for _ in range(NUM_CLAIMS):
        policy = rng.choice(policies)
        lo = max(policy["start_date"], CLAIM_WINDOW_START)
        hi = min(policy["end_date"], BATCH_END)
        claim_date = random_date(rng, lo, hi)  # always within the policy period
        _, _, _, claim_types, _ = PRODUCTS[policy["product_type"]]
        status = weighted(rng, CLAIM_STATUSES)
        si = policy["sum_insured_paise"]
        # Claimed: 2%-40% of sum insured, rounded to whole rupees.
        claimed = rng.randint(si // 50, si * 2 // 5) // 100 * 100
        approved = claimed * rng.randint(60, 100) // 100 // 100 * 100 if status == "APPROVED" else 0
        drafts.append({
            "policy_id": policy["policy_id"],
            "claim_date": claim_date,
            "claim_type": rng.choice(claim_types),
            "claim_status": status,
            "claimed_amount_paise": claimed,
            "approved_amount_paise": approved,
        })
    # Stable sort by date so claim IDs increase chronologically.
    drafts.sort(key=lambda c: c["claim_date"])
    for i, c in enumerate(drafts, start=1):
        c["claim_id"] = f"CLM-{i:07d}"
    return drafts


def split_amount(rng: random.Random, total: int, parts: int) -> list[int]:
    """Split total paise into `parts` positive integers that sum exactly to total."""
    if parts == 1:
        return [total]
    cuts = sorted(rng.sample(range(1, total), parts - 1))
    bounds = [0, *cuts, total]
    return [bounds[i + 1] - bounds[i] for i in range(parts)]


def generate_payments(rng: random.Random, claims: list[dict], policies_by_id: dict) -> list[list[dict]]:
    approved = [c for c in claims if c["claim_status"] == "APPROVED"]
    approved_dates = [c["claim_date"] for c in approved]

    # 1. Assign each payment slot to an approved claim dated on/before the batch date.
    slots: list[tuple[int, int]] = []  # (batch index, index into approved)
    for b in range(NUM_BATCHES):
        batch_date = BATCH_START + timedelta(days=b)
        eligible = bisect_right(approved_dates, batch_date)
        for _ in range(PAYMENTS_PER_BATCH):
            slots.append((b, rng.randrange(eligible)))

    # 2. Split each paid claim's approved amount across its payments.
    slots_per_claim: dict[int, int] = {}
    for _, ci in slots:
        slots_per_claim[ci] = slots_per_claim.get(ci, 0) + 1
    amounts = {ci: split_amount(rng, approved[ci]["approved_amount_paise"], n)
               for ci, n in sorted(slots_per_claim.items())}
    next_part = dict.fromkeys(amounts, 0)

    # 3. Build payment rows, batch by batch.
    alphabet = string.ascii_uppercase + string.digits
    method_options = PAYMENT_METHODS
    batches: list[list[dict]] = [[] for _ in range(NUM_BATCHES)]
    for seq, (b, ci) in enumerate(slots, start=1):
        claim = approved[ci]
        product = policies_by_id[claim["policy_id"]]["product_type"]
        batch_date = BATCH_START + timedelta(days=b)
        amount = amounts[ci][next_part[ci]]
        next_part[ci] += 1
        batches[b].append({
            "payment_id": f"PAY-{seq:08d}",
            "batch_id": f"BATCH-{batch_date:%Y%m%d}",
            "claim_id": claim["claim_id"],
            "payment_date": batch_date,
            "amount_paise": amount,
            "payment_method": weighted(rng, method_options),
            "payee_type": rng.choice(PRODUCTS[product][4]),
            "transaction_ref": "".join(rng.choices(alphabet, k=12)),
        })
    return batches


def write_csv(path: Path, fields: list[str], rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fields, lineterminator="\n")
        writer.writeheader()
        for row in rows:
            writer.writerow({k: (v.isoformat() if isinstance(v, date) else v) for k, v in row.items()})


def main() -> None:
    work_dir = get_work_dir(create=True)
    src = source_dir(work_dir)
    pay_dir = payments_dir(work_dir)
    pay_dir.mkdir(parents=True, exist_ok=True)
    # Remove stale batch files from earlier runs so the batch set is exact.
    for old in pay_dir.glob("payments_*.csv"):
        old.unlink()

    rng = random.Random(SEED)
    policies = generate_policies(rng)
    claims = generate_claims(rng, policies)
    batches = generate_payments(rng, claims, {p["policy_id"]: p for p in policies})

    files = []
    write_csv(src / "policies.csv", POLICY_FIELDS, policies)
    files.append(("policies", src / "policies.csv", len(policies), None))
    write_csv(src / "claims.csv", CLAIM_FIELDS, claims)
    files.append(("claims", src / "claims.csv", len(claims), None))
    for b, rows in enumerate(batches):
        batch_date = BATCH_START + timedelta(days=b)
        path = pay_dir / f"payments_{batch_date:%Y-%m-%d}.csv"
        write_csv(path, PAYMENT_FIELDS, rows)
        files.append(("payments", path, len(rows), batch_date))

    manifest = {
        "seed": SEED,
        "batch_start": BATCH_START.isoformat(),
        "num_batches": NUM_BATCHES,
        "payments_per_batch": PAYMENTS_PER_BATCH,
        "files": [
            {
                "kind": kind,
                "path": path.relative_to(src).as_posix(),
                "rows": rows,
                "batch_date": bd.isoformat() if bd else None,
                "sha256": sha256_file(path),
            }
            for kind, path, rows, bd in files
        ],
    }
    manifest_path(work_dir).write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")

    total_paise = sum(p["amount_paise"] for rows in batches for p in rows)
    print(f"Work dir      : {work_dir}")
    print(f"Policies      : {len(policies):,}")
    print(f"Claims        : {len(claims):,}")
    print(f"Payment files : {len(batches)}")
    print(f"Payments      : {sum(len(r) for r in batches):,}")
    print(f"Payment total : {total_paise:,} paise")


if __name__ == "__main__":
    main()
