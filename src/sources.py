"""dlt sources and per-squad transformers for the unified payment pipeline."""

from __future__ import annotations

import csv
import os
import uuid
from collections.abc import Iterator
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any

import dlt

DATA_DIR = Path(os.environ.get("DATA_DIR", Path(__file__).resolve().parent.parent / "data" / "raw"))
STATUS_MAP_CARDS = {"approved": "completed", "declined": "failed", "pending": "pending"}
STATUS_MAP_TRANSFERS = {"COMPLETED": "completed", "FAILED": "failed", "PROCESSING": "pending"}
STATUS_MAP_BILLS = {"success": "completed", "failed": "failed", "in_progress": "pending"}


def _read_csv(filename: str) -> list[dict[str, str]]:
    with open(DATA_DIR / filename, newline="") as f:
        return list(csv.DictReader(f))


def _base(
    system: str,
    src_id: str,
    cust: str,
    amt: str,
    cur: str,
    ts: datetime,
    status: str,
    ptype: str,
    method: str,
    meta: dict,
    **extra,
) -> dict[str, Any]:
    return {
        "event_id": str(uuid.uuid4()),
        "source_system": system,
        "source_event_id": src_id,
        "customer_id": cust,
        "counterparty_id": extra.get("cp_id"),
        "counterparty_name": extra.get("cp_name"),
        "amount": Decimal(amt),
        "currency": cur.upper(),
        "event_timestamp": ts,
        "status": status,
        "payment_type": ptype,
        "payment_method": method,
        "metadata": meta,
        "schema_version": 2,
    }


def _transform_card(r: dict[str, str]) -> dict[str, Any]:
    return _base(
        "cards",
        r["txn_id"],
        r["customer_id"],
        r["txn_amount"],
        r["txn_currency"],
        datetime.fromisoformat(r["txn_timestamp"].replace("Z", "+00:00")),
        STATUS_MAP_CARDS[r["txn_status"]],
        "card_transaction",
        r["card_type"],
        {"card_number": r["card_number"], "mcc_code": r["mcc_code"]},
        cp_name=r["merchant_name"],
    )


def _transform_transfer(r: dict[str, str]) -> dict[str, Any]:
    return _base(
        "transfers",
        r["transfer_id"],
        r["sender_id"],
        r["amount"],
        r["ccy"],
        datetime.fromtimestamp(int(r["created_at"]) / 1000, tz=timezone.utc),
        STATUS_MAP_TRANSFERS[r["state"]],
        "transfer",
        r["transfer_type"],
        {"reference_note": r["reference_note"]},
        cp_id=r["receiver_id"],
    )


def _transform_bill(r: dict[str, str]) -> dict[str, Any]:
    dt = datetime.strptime(r["payment_date"], "%d/%m/%Y %H:%M").replace(tzinfo=timezone.utc)
    return _base(
        "bill_payments",
        r["payment_id"],
        r["user_id"],
        r["pay_amount"],
        r["currency_code"],
        dt,
        STATUS_MAP_BILLS[r["payment_status"]],
        "bill_payment",
        r["bill_category"],
        {"account_number": r["account_number"]},
        cp_id=r["biller_code"],
        cp_name=r["biller_name"],
    )


SOURCES = [
    ("cards_events.csv", _transform_card),
    ("transfers_events.csv", _transform_transfer),
    ("bill_payments_events.csv", _transform_bill),
]


@dlt.source(name="mal_payments")
def payment_sources():
    """Top-level dlt source that yields the unified payment_events resource."""
    return payment_events_resource()


@dlt.resource(name="payment_events", write_disposition="replace")
def payment_events_resource() -> Iterator[dict[str, Any]]:
    """Single resource that merges all three squad sources into one stream."""
    for filename, transform_fn in SOURCES:
        for row in _read_csv(filename):
            yield transform_fn(row)
