"""Canonical payment event schema with Pydantic v2 models and versioning."""

from __future__ import annotations

import uuid
from datetime import datetime
from decimal import Decimal
from typing import Any, Literal

from pydantic import BaseModel, Field, field_validator


class PaymentEventV1(BaseModel):
    """Original flat schema -- kept for backward compatibility and migration demo."""

    event_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    source_system: Literal["cards", "transfers", "bill_payments"]
    source_event_id: str
    customer_id: str
    amount: Decimal
    currency: str
    event_timestamp: datetime
    status: Literal["completed", "failed", "pending"]
    payment_type: Literal["card_transaction", "transfer", "bill_payment"]
    payment_method: str | None = None
    schema_version: int = 1


class PaymentEventV2(BaseModel):
    """Current canonical schema -- adds counterparty model and metadata dict."""

    event_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    source_system: Literal["cards", "transfers", "bill_payments"]
    source_event_id: str
    customer_id: str
    counterparty_id: str | None = None
    counterparty_name: str | None = None
    amount: Decimal
    currency: str
    event_timestamp: datetime
    status: Literal["completed", "failed", "pending"]
    payment_type: Literal["card_transaction", "transfer", "bill_payment"]
    payment_method: str | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)
    schema_version: int = 2

    @field_validator("currency")
    @classmethod
    def currency_must_be_iso(cls, v: str) -> str:
        if len(v) != 3 or not v.isalpha():
            raise ValueError(f"Currency must be 3-letter ISO 4217 code, got '{v}'")
        return v.upper()

    @field_validator("amount")
    @classmethod
    def amount_must_be_non_negative(cls, v: Decimal) -> Decimal:
        if v < 0:
            raise ValueError(f"Amount must be non-negative, got {v}")
        return v


PaymentEvent = PaymentEventV2

SCHEMA_REGISTRY: dict[int, type[BaseModel]] = {
    1: PaymentEventV1,
    2: PaymentEventV2,
}
