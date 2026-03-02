"""
Mock data generator for the warehouse pipeline.

Produces three datasets with calibrated dirty-data ratios:
  - web_events.json         40k rows  (~5% null user_id, ~3% dup session_id,
                                        50/50 ISO vs epoch timestamps)
  - crm_users.csv           30k rows  (~2% dup emails, ~30% full state names,
                                        4 phone formats)
  - sales_transactions.parquet  30k rows  (~1% negative totals, ~2% future dates,
                                            ~2% dup transaction_ids)

All generators are seeded (Faker seed=42, random.seed=42) for reproducibility.
"""
from __future__ import annotations

import json
import random
from pathlib import Path

import pandas as pd
from faker import Faker

fake = Faker()
Faker.seed(42)
random.seed(42)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
EVENT_TYPES = ["page_view", "click", "form_submit", "scroll", "video_play", "search"]
DEVICE_TYPES = ["desktop", "mobile", "tablet"]
PLAN_TIERS = ["free", "starter", "pro", "enterprise"]
CATEGORIES = ["Electronics", "Clothing", "Home & Garden", "Sports", "Books", "Toys"]
REGIONS = ["North", "South", "East", "West", "Central"]

US_STATES_ABBR = [
    "AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "FL", "GA",
    "HI", "ID", "IL", "IN", "IA", "KS", "KY", "LA", "ME", "MD",
    "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH", "NJ",
    "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI", "SC",
    "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV", "WI", "WY",
]

STATE_FULL_NAMES = {
    "AL": "Alabama", "AK": "Alaska", "AZ": "Arizona", "AR": "Arkansas",
    "CA": "California", "CO": "Colorado", "CT": "Connecticut", "DE": "Delaware",
    "FL": "Florida", "GA": "Georgia", "HI": "Hawaii", "ID": "Idaho",
    "IL": "Illinois", "IN": "Indiana", "IA": "Iowa", "KS": "Kansas",
    "KY": "Kentucky", "LA": "Louisiana", "ME": "Maine", "MD": "Maryland",
    "MA": "Massachusetts", "MI": "Michigan", "MN": "Minnesota", "MS": "Mississippi",
    "MO": "Missouri", "MT": "Montana", "NE": "Nebraska", "NV": "Nevada",
    "NH": "New Hampshire", "NJ": "New Jersey", "NM": "New Mexico", "NY": "New York",
    "NC": "North Carolina", "ND": "North Dakota", "OH": "Ohio", "OK": "Oklahoma",
    "OR": "Oregon", "PA": "Pennsylvania", "RI": "Rhode Island", "SC": "South Carolina",
    "SD": "South Dakota", "TN": "Tennessee", "TX": "Texas", "UT": "Utah",
    "VT": "Vermont", "VA": "Virginia", "WA": "Washington", "WV": "West Virginia",
    "WI": "Wisconsin", "WY": "Wyoming",
}

PRODUCTS = [
    ("P001", "Laptop Pro 15", "Electronics", 1299.99),
    ("P002", "Wireless Mouse", "Electronics", 29.99),
    ("P003", "Standing Desk", "Home & Garden", 449.99),
    ("P004", "Running Shoes", "Sports", 119.99),
    ("P005", "Python Cookbook", "Books", 49.99),
    ("P006", "Yoga Mat", "Sports", 34.99),
    ("P007", "Noise-Cancel Headphones", "Electronics", 299.99),
    ("P008", "Coffee Maker", "Home & Garden", 89.99),
    ("P009", "Winter Jacket", "Clothing", 189.99),
    ("P010", "Action Figure Set", "Toys", 24.99),
    ("P011", "4K Monitor", "Electronics", 599.99),
    ("P012", "Office Chair", "Home & Garden", 349.99),
    ("P013", "Basketball", "Sports", 39.99),
    ("P014", "Data Science Book", "Books", 54.99),
    ("P015", "Bluetooth Speaker", "Electronics", 79.99),
]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _phone_variants(fake_instance: Faker) -> str:
    """Return a phone number in one of 4 dirty formats."""
    digits = "".join(filter(str.isdigit, fake_instance.phone_number()))[-10:]
    fmt = random.randint(0, 3)
    if fmt == 0:
        return f"({digits[:3]}) {digits[3:6]}-{digits[6:]}"
    elif fmt == 1:
        return f"{digits[:3]}-{digits[3:6]}-{digits[6:]}"
    elif fmt == 2:
        return f"+1{digits}"
    else:
        return digits


def _iso_or_epoch(dt_obj) -> str | int:
    """Return timestamp as ISO string or Unix epoch integer (50/50 split)."""
    if random.random() < 0.5:
        return dt_obj.isoformat()
    return int(dt_obj.timestamp())


# ---------------------------------------------------------------------------
# Dataset generators
# ---------------------------------------------------------------------------

def generate_web_events(n: int = 40_000) -> list[dict]:
    """Generate web events with ~5% null user_id, ~3% dup session_id, mixed timestamps."""
    user_pool = [fake.uuid4() for _ in range(5_000)]
    session_pool = [fake.uuid4() for _ in range(int(n * 0.97))]  # 3% will be reused

    rows = []
    for _ in range(n):
        user_id = None if random.random() < 0.05 else random.choice(user_pool)
        session_id = (
            random.choice(session_pool[:100])          # reuse from small bucket → duplicates
            if random.random() < 0.03
            else random.choice(session_pool)
        )
        ts = fake.date_time_between(start_date="-3y", end_date="now")
        rows.append({
            "user_id": user_id,
            "session_id": session_id,
            "event_type": random.choice(EVENT_TYPES),
            "page_url": fake.uri(),
            "referrer": fake.uri() if random.random() > 0.3 else None,
            "device_type": random.choice(DEVICE_TYPES),
            "timestamp": _iso_or_epoch(ts),
            "country": fake.country_code(),
        })
    return rows


def generate_crm_users(n: int = 30_000) -> pd.DataFrame:
    """Generate CRM users with ~2% dup emails, ~30% full state names, 4 phone formats."""
    email_pool = [fake.unique.email() for _ in range(int(n * 0.98))]
    fake.unique.clear()

    records = []
    for i in range(n):
        state_abbr = random.choice(US_STATES_ABBR)
        # ~30% full state name instead of abbreviation
        state_val = STATE_FULL_NAMES[state_abbr] if random.random() < 0.30 else state_abbr

        # ~2% duplicate emails
        if random.random() < 0.02 and email_pool:
            email = random.choice(email_pool[:200])
        else:
            email = email_pool[i % len(email_pool)]

        records.append({
            "user_id": fake.uuid4(),
            "first_name": fake.first_name(),
            "last_name": fake.last_name(),
            "email": email,
            "phone": _phone_variants(fake),
            "state": state_val,
            "city": fake.city(),
            "signup_date": fake.date_between(start_date="-5y", end_date="today").isoformat(),
            "plan_tier": random.choice(PLAN_TIERS),
        })
    return pd.DataFrame(records)


def generate_sales_transactions(n: int = 30_000) -> pd.DataFrame:
    """Generate sales with ~1% negative totals, ~2% future dates, ~2% dup transaction_ids."""
    txn_pool = [fake.uuid4() for _ in range(int(n * 0.98))]
    today = pd.Timestamp.today()
    future_cutoff = today + pd.Timedelta(days=180)

    records = []
    for i in range(n):
        product = random.choice(PRODUCTS)
        qty = random.randint(1, 10)
        unit_price = product[3] * random.uniform(0.85, 1.15)
        total = round(qty * unit_price, 4)

        # ~1% refunds (negative totals)
        if random.random() < 0.01:
            total = -abs(total)

        # ~2% future dates
        if random.random() < 0.02:
            txn_date = fake.date_between(
                start_date=today.date(), end_date=future_cutoff.date()
            ).isoformat()
        else:
            txn_date = fake.date_between(start_date="-3y", end_date="today").isoformat()

        # ~2% duplicate transaction IDs
        if random.random() < 0.02 and txn_pool:
            txn_id = random.choice(txn_pool[:100])
        else:
            txn_id = txn_pool[i % len(txn_pool)]

        records.append({
            "transaction_id": txn_id,
            "user_id": fake.uuid4(),
            "product_id": product[0],
            "product_name": product[1],
            "category": product[2],
            "quantity": qty,
            "unit_price": round(unit_price, 4),
            "total_amount": total,
            "region": random.choice(REGIONS),
            "transaction_date": txn_date,
        })
    return pd.DataFrame(records)


# ---------------------------------------------------------------------------
# Entry point — write files to RAW_DATA_DIR
# ---------------------------------------------------------------------------

def generate_all(output_dir: Path) -> None:
    """Generate all three datasets and write to *output_dir*."""
    output_dir.mkdir(parents=True, exist_ok=True)

    print("  Generating web_events.json (40k rows)…")
    events = generate_web_events(40_000)
    with open(output_dir / "web_events.json", "w", encoding="utf-8") as fh:
        json.dump(events, fh, default=str)
    print(f"    → {len(events):,} rows written")

    print("  Generating crm_users.csv (30k rows)…")
    users_df = generate_crm_users(30_000)
    users_df.to_csv(output_dir / "crm_users.csv", index=False)
    print(f"    → {len(users_df):,} rows written")

    print("  Generating sales_transactions.parquet (30k rows)…")
    sales_df = generate_sales_transactions(30_000)
    sales_df.to_parquet(output_dir / "sales_transactions.parquet", index=False)
    print(f"    → {len(sales_df):,} rows written")
