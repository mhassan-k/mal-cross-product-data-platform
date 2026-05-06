"""Standalone pipeline runner -- works without Airflow for quick local execution."""
from __future__ import annotations
import json, logging, os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
import dlt, duckdb
from src.contracts import ensure_latest_version
from src.schema import PaymentEventV2
from src.sources import payment_sources

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)
OUTPUT_DIR = Path(os.environ.get("OUTPUT_DIR", Path(__file__).resolve().parent.parent / "data" / "output"))
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
DUCKDB_PATH = os.environ.get("DUCKDB_PATH", "mal_payments.duckdb")

def validate_records(records: list[dict]) -> tuple[list[dict], list[dict]]:
    valid, errors = [], []
    for rec in records:
        try:
            rec = ensure_latest_version(rec)
            PaymentEventV2.model_validate(rec)
            valid.append(rec)
        except Exception as e:
            errors.append({"record": rec, "error": str(e)})
    return valid, errors

def run() -> dict[str, int]:
    logger.info("Starting unified payment pipeline")
    pipeline = dlt.pipeline(pipeline_name="mal_payments",
        destination=dlt.destinations.duckdb(DUCKDB_PATH), dataset_name="payments_data")
    records: list[dict] = list(payment_sources().resources["payment_events"])
    valid, errors = validate_records(records)
    logger.info("Validated %d records, %d errors", len(valid), len(errors))
    if errors:
        (OUTPUT_DIR / "errors.json").write_text(json.dumps(errors, indent=2, default=str))
    if valid:
        pipeline.run(valid, table_name="payment_events")
        con = duckdb.connect(DUCKDB_PATH)
        con.execute(f"COPY (SELECT * FROM payments_data.payment_events) "
                    f"TO '{OUTPUT_DIR}/payment_events.parquet' (FORMAT PARQUET)")
        con.close()
    stats = {"total": len(records), "valid": len(valid), "errors": len(errors),
             "timestamp": datetime.now(timezone.utc).isoformat()}
    logger.info("Pipeline complete: %s", stats)
    return stats

if __name__ == "__main__":
    run()
