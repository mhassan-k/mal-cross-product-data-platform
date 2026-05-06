"""Data contract versioning: v1 -> v2 migration and schema registry."""
from __future__ import annotations
from typing import Any
from pydantic import BaseModel
from src.schema import SCHEMA_REGISTRY, PaymentEventV1, PaymentEventV2

def migrate_v1_to_v2(record: dict[str, Any]) -> dict[str, Any]:
    """Upgrade a v1 record to v2 by adding counterparty and metadata fields."""
    v1 = PaymentEventV1.model_validate(record)
    return PaymentEventV2(
        event_id=v1.event_id, source_system=v1.source_system,
        source_event_id=v1.source_event_id, customer_id=v1.customer_id,
        counterparty_id=None, counterparty_name=None,
        amount=v1.amount, currency=v1.currency,
        event_timestamp=v1.event_timestamp, status=v1.status,
        payment_type=v1.payment_type, payment_method=v1.payment_method,
        metadata={}, schema_version=2,
    ).model_dump()

def ensure_latest_version(record: dict[str, Any]) -> dict[str, Any]:
    """Migrate a record to the latest schema version."""
    version = record.get("schema_version", 1)
    if version == 2: return record
    if version == 1: return migrate_v1_to_v2(record)
    raise ValueError(f"Unknown schema version: {version}")

def validate_contract(record: dict[str, Any], version: int = 2) -> BaseModel:
    """Validate a record against a specific schema version."""
    model_cls = SCHEMA_REGISTRY.get(version)
    if model_cls is None: raise ValueError(f"No schema registered for version {version}")
    return model_cls.model_validate(record)
