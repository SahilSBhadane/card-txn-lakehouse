"""Central config. Everything is overridable with environment variables."""
import os
from pathlib import Path

DATA_ROOT = Path(os.environ.get("LAKEHOUSE_DATA", Path(__file__).resolve().parents[2] / "data"))

LANDING = DATA_ROOT / "landing"      # raw files as a source system would drop them
BRONZE = DATA_ROOT / "bronze"        # raw, typed as strings, + ingestion metadata
SILVER = DATA_ROOT / "silver"        # cleaned, deduped, conformed
GOLD = DATA_ROOT / "gold"            # business-ready tables
QUARANTINE = DATA_ROOT / "quarantine"
DQ_REPORTS = DATA_ROOT / "_dq"

N_CUSTOMERS = int(os.environ.get("N_CUSTOMERS", 20_000))
TXNS_PER_DAY = int(os.environ.get("TXNS_PER_DAY", 200_000))

# Data-quality gate: fail the run if more than this share of rows is quarantined.
MAX_QUARANTINE_RATE = float(os.environ.get("MAX_QUARANTINE_RATE", 0.05))

# Static FX table (demo values) used to normalise amounts to INR.
FX_TO_INR = {"INR": 1.0, "USD": 83.0, "EUR": 90.0, "GBP": 105.0, "AED": 22.6, "SGD": 61.5}

# Fraud-rule thresholds
VELOCITY_WINDOW_SEC = 600        # 10 minutes
VELOCITY_MAX_TXNS = 5            # >= 5 txns on one card in 10 minutes
SPIKE_MULTIPLIER = 10.0          # txn > 10x the customer's trailing average
SPIKE_LOOKBACK_DAYS = 30
SPIKE_MIN_AMOUNT_INR = 75_000     # a 10x "spike" on a ₹300 average is noise, not fraud
FOREIGN_HIGH_AMOUNT_INR = 100_000
