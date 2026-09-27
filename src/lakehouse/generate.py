"""Synthetic source system.

Drops two feeds into landing/ for a given business date (ds), the way an upstream system would:
  * transactions/ds=YYYY-MM-DD/part-0000.jsonl  - card transactions (JSON lines)
  * customers/ds=YYYY-MM-DD/customers.csv        - full daily customer snapshot (CSV)

Real-world mess is injected on purpose so the silver layer has something to fix:
  duplicate txn_ids, null customer ids, negative amounts, unknown currencies,
  bad timestamps, late-arriving events and bursts of rapid transactions (for fraud rules).
Everything is seeded by ds, so re-running a day produces identical files (idempotent).
"""
from __future__ import annotations

import csv
import json
import random
from datetime import date, datetime, timedelta, timezone

from lakehouse import config

CITIES = ["Mumbai", "Pune", "Bengaluru", "Hyderabad", "Chennai", "Delhi", "Kolkata", "Ahmedabad"]
SEGMENTS = ["mass", "affluent", "premium"]
RISK_TIERS = ["low", "medium", "high"]
CATEGORIES = ["grocery", "fuel", "travel", "electronics", "dining", "utilities", "fashion", "pharmacy"]
CHANNELS = ["pos", "ecom", "contactless"]
EPOCH = date(2026, 6, 1)  # history for customer attribute changes starts here
CURRENCY_WEIGHTS ={"INR": 0.93, "USD": 0.03, "EUR": 0.01, "GBP": 0.01, "AED": 0.01, "SGD": 0.01}


def _customers(ds: date, n: int) -> list[dict]:
    """Stable base attributes per customer, with ~1% of customers changing segment/city on each day."""
    rows = []
    for cid in range(1, n + 1):
        base = random.Random(cid)
        city = base.choice(CITIES)
        segment = base.choices(SEGMENTS, weights=[0.7, 0.25, 0.05])[0]
        risk = base.choices(RISK_TIERS, weights=[0.8, 0.15, 0.05])[0]
        # Deterministic "life events": ~1% of customers move city and ~0.5% change segment each day.
        # One seeded stream per customer, replayed up to ds, so any day's snapshot is reproducible.
        ev = random.Random(f"ev-{cid}")
        for _ in range(0, (ds - EPOCH).days + 1):
            move, seg_change, new_city, new_seg = ev.random(), ev.random(), ev.choice(CITIES), ev.choice(SEGMENTS)
            if move < 0.01:
                city = new_city
            if seg_change < 0.005:
                segment = new_seg
        rows.append({
            "customer_id": f"C{cid:07d}",
            "full_name": f"Customer {cid}",
            "city": city,
            "segment": segment,
            "risk_tier": risk,
            "card_id": f"K{cid:08d}",
        })
    return rows


def _amount(rng: random.Random, category: str) -> float:
    scale = {"grocery": 900, "fuel": 1800, "travel": 12000, "electronics": 15000, "dining": 1400,
             "utilities": 2500, "fashion": 3500, "pharmacy": 700}[category]
    return round(rng.lognormvariate(0, 0.8) * scale, 2)


def generate(ds_str: str, n_customers: int | None = None, txns_per_day: int | None = None) -> dict:
    ds = date.fromisoformat(ds_str)
    n_customers = n_customers or config.N_CUSTOMERS
    txns_per_day = txns_per_day or config.TXNS_PER_DAY
    rng = random.Random(f"txn-{ds_str}")

    # --- customers snapshot (CSV) ---
    cust_dir = config.LANDING / "customers" / f"ds={ds_str}"
    cust_dir.mkdir(parents=True, exist_ok=True)
    customers = _customers(ds, n_customers)
    with open(cust_dir / "customers.csv", "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(customers[0].keys()))
        w.writeheader()
        w.writerows(customers)

    # --- transactions (JSON lines) ---
    txn_dir = config.LANDING / "transactions" / f"ds={ds_str}"
    txn_dir.mkdir(parents=True, exist_ok=True)
    day_start = datetime(ds.year, ds.month, ds.day, tzinfo=timezone.utc)
    currencies, cur_w = zip(*CURRENCY_WEIGHTS.items())
    stats = {"rows": 0, "duplicates": 0, "null_customer": 0, "negative_amount": 0,
             "bad_currency": 0, "bad_timestamp": 0, "late": 0, "burst_txns": 0}

    with open(txn_dir / "part-0000.jsonl", "w") as f:
        def emit(rec: dict) -> None:
            f.write(json.dumps(rec) + "\n")
            stats["rows"] += 1

        for i in range(txns_per_day):
            cid = rng.randint(1, n_customers)
            category = rng.choice(CATEGORIES)
            ts = day_start + timedelta(seconds=rng.randint(0, 86_399))
            if rng.random() < 0.002:  # late-arriving event from yesterday
                ts -= timedelta(days=1)
                stats["late"] += 1
            rec = {
                "txn_id": f"T{ds_str.replace('-', '')}{i:08d}",
                "card_id": f"K{cid:08d}",
                "customer_id": f"C{cid:07d}",
                "merchant_id": f"M{rng.randint(1, 5000):05d}",
                "category": category,
                "channel": rng.choice(CHANNELS),
                "currency": (cur := rng.choices(currencies, weights=cur_w)[0]),
                # amount is in the transaction's own currency (INR-scale amount / FX rate)
                "amount": round(_amount(rng, category) / config.FX_TO_INR[cur], 2),
                "event_time": ts.strftime("%Y-%m-%dT%H:%M:%SZ"),
            }
            # ---- inject dirty data ----
            r = rng.random()
            if r < 0.005:
                rec["customer_id"] = None
                stats["null_customer"] += 1
            elif r < 0.008:
                rec["amount"] = -abs(rec["amount"])
                stats["negative_amount"] += 1
            elif r < 0.010:
                rec["currency"] = "XYZ"
                stats["bad_currency"] += 1
            elif r < 0.011:
                rec["event_time"] = "not-a-timestamp"
                stats["bad_timestamp"] += 1
            if rng.random() < 0.01:  # upstream resends ~1% of events (duplicate txn_id)
                emit(rec)
                stats["duplicates"] += 1
            if rec["currency"] in ("inr", "INR") and rng.random() < 0.05:
                rec["currency"] = " inr "  # messy formatting to standardise
            emit(rec)

        # A few bursty cards (velocity fraud pattern): 6-8 txns within ~5 minutes.
        for b in range(max(1, txns_per_day // 20_000)):
            cid = rng.randint(1, n_customers)
            burst_start = day_start + timedelta(seconds=rng.randint(0, 80_000))
            for k in range(rng.randint(6, 8)):
                emit({
                    "txn_id": f"T{ds_str.replace('-', '')}B{b:03d}{k:02d}",
                    "card_id": f"K{cid:08d}", "customer_id": f"C{cid:07d}",
                    "merchant_id": f"M{rng.randint(1, 5000):05d}", "category": "electronics",
                    "channel": "ecom", "amount": round(rng.uniform(4000, 9000), 2), "currency": "INR",
                    "event_time": (burst_start + timedelta(seconds=40 * k)).strftime("%Y-%m-%dT%H:%M:%SZ"),
                })
                stats["burst_txns"] += 1
    return stats


if __name__ == "__main__":
    import sys
    print(generate(sys.argv[1]))
