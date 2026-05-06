"""Unit tests for schema validation, transformers, and contracts."""

from __future__ import annotations

import uuid
from datetime import datetime, timezone
from decimal import Decimal

import pytest

from src.contracts import ensure_latest_version, migrate_v1_to_v2, validate_contract
from src.schema import PaymentEventV1, PaymentEventV2
from src.sources import _transform_bill, _transform_card, _transform_transfer

V2_BASE = dict(
    event_id=str(uuid.uuid4()),
    source_system="cards",
    source_event_id="TXN-999",
    customer_id="CUST-1001",
    counterparty_id=None,
    counterparty_name="Test Merchant",
    amount=Decimal("100.00"),
    currency="SAR",
    event_timestamp=datetime(2025, 3, 1, 9, 0, tzinfo=timezone.utc),
    status="completed",
    payment_type="card_transaction",
    payment_method="debit",
    metadata={"mcc_code": "5411"},
    schema_version=2,
)


def _v2(**kw):
    return {**V2_BASE, **kw}


class TestSchema:
    def test_valid_v2(self):
        assert PaymentEventV2.model_validate(_v2()).schema_version == 2

    def test_bad_currency(self):
        with pytest.raises(ValueError):
            PaymentEventV2.model_validate(_v2(currency="INVALID"))

    def test_negative_amount(self):
        with pytest.raises(ValueError):
            PaymentEventV2.model_validate(_v2(amount=Decimal("-50")))

    def test_bad_status(self):
        with pytest.raises(ValueError):
            PaymentEventV2.model_validate(_v2(status="unknown"))

    def test_v1(self):
        v1 = PaymentEventV1(
            source_system="cards",
            source_event_id="TXN-001",
            customer_id="CUST-1001",
            amount=Decimal("250"),
            currency="SAR",
            event_timestamp=datetime(2025, 3, 1, tzinfo=timezone.utc),
            status="completed",
            payment_type="card_transaction",
        )
        assert v1.schema_version == 1


class TestTransformers:
    def test_card(self):
        r = _transform_card(
            dict(
                txn_id="TXN-001",
                card_number="4532XXXX1234",
                customer_id="CUST-1001",
                merchant_name="Jarir",
                txn_amount="250.00",
                txn_currency="SAR",
                txn_timestamp="2025-03-01T09:15:00Z",
                txn_status="approved",
                card_type="debit",
                mcc_code="5942",
            )
        )
        assert r["source_system"] == "cards" and r["status"] == "completed"

    def test_transfer(self):
        r = _transform_transfer(
            dict(
                transfer_id="TRF-2001",
                sender_id="CUST-1001",
                receiver_id="CUST-1002",
                amount="500.00",
                ccy="SAR",
                created_at="1709290800000",
                state="COMPLETED",
                transfer_type="internal",
                reference_note="Rent",
            )
        )
        assert r["customer_id"] == "CUST-1001" and r["counterparty_id"] == "CUST-1002"

    def test_bill(self):
        r = _transform_bill(
            dict(
                payment_id="BILL-3001",
                user_id="CUST-1001",
                biller_code="STC-001",
                biller_name="STC",
                pay_amount="299.00",
                currency_code="SAR",
                payment_date="01/03/2025 09:00",
                payment_status="success",
                bill_category="telecom",
                account_number="05X",
            )
        )
        assert r["source_system"] == "bill_payments" and r["payment_method"] == "telecom"


class TestContracts:
    def test_v1_to_v2_migration(self):
        v2 = migrate_v1_to_v2(
            dict(
                event_id=str(uuid.uuid4()),
                source_system="cards",
                source_event_id="TXN-001",
                customer_id="CUST-1001",
                amount=Decimal("250"),
                currency="SAR",
                event_timestamp=datetime(2025, 3, 1, tzinfo=timezone.utc),
                status="completed",
                payment_type="card_transaction",
                schema_version=1,
            )
        )
        assert v2["schema_version"] == 2 and "metadata" in v2

    def test_passthrough_v2(self):
        assert ensure_latest_version(_v2())["schema_version"] == 2

    def test_validate_contract(self):
        assert isinstance(validate_contract(_v2(), version=2), PaymentEventV2)
