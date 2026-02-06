import json
import os
import random
import string
import time
from datetime import datetime, timezone
from confluent_kafka import Producer

# ---------- Configuration ---------
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "fintech_transactions")

# How many events to send total (default 200)
TOTAL_EVENTS = int(os.getenv("TOTAL_EVENTS", "200"))

# Events per second (default 5)
EVENTS_PER_SEC = float(os.getenv("EVENTS_PER_SEC", "5"))

# Percentage of events to intentionally corrupt (default 10%)
BAD_PCT = int(os.getenv("BAD_PCT", "10"))

SCHEMA_VERSION = 1
SOURCE_SYSTEM = "payments_api_simulator_v1"

PAYMENT_METHODS = ["card", "ach", "wire", "wallet"]
TXN_TYPES = ["purchase", "refund", "payout", "transfer"]
STATUSES = ["initiated", "authorized", "failed", "settled"]
FAIL_REASONS = ["insufficient_funds", "suspected_fraud", "network_error", "invalid_account", "limit_exceeded"]

EVENT_TYPES = [
    "transaction_created",
    "transaction_authorized",
    "transaction_failed",
    "transaction_settled",
    "refund_initiated",
    "refund_settled",
]

CURRENCIES = ["USD", "INR", "EUR"]

# ---------- Helpers ----------
def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")

def rand_id(prefix: str, n: int = 10) -> str:
    return prefix + "_" + "".join(random.choices(string.ascii_lowercase + string.digits, k=n))

def money_str(min_amt=1, max_amt=5000) -> str:
    # Money as a string with 2 decimals (important for fintech)
    cents = random.randint(min_amt * 100, max_amt * 100)
    return f"{cents/100:.2f}"

def build_valid_event() -> dict:
    event_type = random.choice(EVENT_TYPES)

    txn_id = rand_id("txn", 12)
    user_id = rand_id("usr", 8)
    merchant_id = rand_id("mrc", 6)

    # Map event_type -> status (simple lifecycle)
    if event_type == "transaction_created":
        status = "initiated"
        failure_reason = None
    elif event_type == "transaction_authorized":
        status = "authorized"
        failure_reason = None
    elif event_type == "transaction_failed":
        status = "failed"
        failure_reason = random.choice(FAIL_REASONS)
    elif event_type == "transaction_settled":
        status = "settled"
        failure_reason = None
    elif event_type == "refund_initiated":
        status = "initiated"
        failure_reason = None
    else:  # refund_settled
        status = "settled"
        failure_reason = None

    e = {
        "event_id": rand_id("evt", 12),
        "transaction_id": txn_id,
        "user_id": user_id,
        "merchant_id": merchant_id,
        "amount": money_str(),
        "currency": random.choice(CURRENCIES),
        "payment_method": random.choice(PAYMENT_METHODS),
        "transaction_type": random.choice(TXN_TYPES),
        "status": status,
        "failure_reason": failure_reason,
        "event_time": utc_now_iso(),
        "ingested_at": utc_now_iso(),
        "risk_score": random.randint(0, 100),
        "source_system": SOURCE_SYSTEM,
        "schema_version": SCHEMA_VERSION,

    }
    return e

def corrupt_event(e: dict) -> str:
    """
    Return a JSON string that is intentionally schema-inconsistent.
    We do multiple corruption modes to test defensive parsing.
    """
    mode = random.choice([
        "missing_required",
        "wrong_type",
        "bad_enum",
        "extra_field",
        "malformed_json"
    ])

    if mode == "missing_required":
        # removes a required field
        key = random.choice(["amount", "currency", "status", "event_time", "transaction_id"])
        e.pop(key, None)
        return json.dumps(e)

    if mode == "wrong_type":
        # amount must be string; makes it a number
        e["amount"] = float(e.get("amount", "10.00"))
        return json.dumps(e)

    if mode == "bad_enum":
        # invalid currency or status
        if random.random() < 0.5:
            e["currency"] = "usd"  # wrong case
        else:
            e["status"] = "authorised"  # wrong spelling
        return json.dumps(e)

    if mode == "extra_field":
        # additionalProperties=false should reject this later
        e["unexpected_field"] = {"nested": True}
        return json.dumps(e)

    # malformed_json
    good = json.dumps(e)
    return good[:-1]  # cuts last char to break JSON

def delivery_report(err, msg):
    if err is not None:
        print(f"[DELIVERY-FAIL] {err}")
    # else: successful sends keeps quiet

# ---------- Main ----------
def main():
    p = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP})
    bad_every = max(1, int(100 / max(1, BAD_PCT)))  # e.g., 10% => every 10th event

    delay = 1.0 / max(0.1, EVENTS_PER_SEC)

    for i in range(1, TOTAL_EVENTS + 1):
        event = build_valid_event()

        if (i % bad_every) == 0:
            payload = corrupt_event(event)
            kind = "BAD"
        else:
            payload = json.dumps(event)
            kind = "OK"

        # Keying by transaction_id improves Kafka ordering for same transaction
        key = event.get("transaction_id", "unknown").encode("utf-8")

        p.produce(TOPIC, value=payload.encode("utf-8"), key=key, callback=delivery_report)
        p.poll(0)

        if i % 25 == 0:
            p.flush(5)
            print(f"Sent {i}/{TOTAL_EVENTS} events (mix of OK/BAD).")

        time.sleep(delay)

    p.flush(10)
    print("Done.")

if __name__ == "__main__":
    main()
