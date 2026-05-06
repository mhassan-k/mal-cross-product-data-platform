"""Airflow DAG for the unified payment pipeline using TaskFlow API."""

from __future__ import annotations

import json
import logging
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from airflow.decorators import dag, task

PROJECT_ROOT = Path(os.environ.get("AIRFLOW_HOME", Path(__file__).resolve().parent.parent))
sys.path.insert(0, str(PROJECT_ROOT))

import dlt
import duckdb

from src.contracts import ensure_latest_version
from src.schema import PaymentEventV2
from src.sources import payment_events_resource

DUCKDB_PATH = os.environ.get("DUCKDB_PATH", "mal_payments.duckdb")
OUTPUT_DIR = Path(os.environ.get("OUTPUT_DIR", PROJECT_ROOT / "data" / "output"))
SQL_DIR = Path(os.environ.get("SQL_DIR", PROJECT_ROOT / "sql"))
logger = logging.getLogger(__name__)


@dag(
    dag_id="mal_payment_pipeline",
    schedule="@daily",
    start_date=datetime(2025, 3, 1),
    catchup=False,
    tags=["payments", "mal"],
)
def payment_pipeline():

    @task(task_id="extract_squad_payments")
    def extract() -> list[dict[str, Any]]:
        records = []
        for rec in payment_events_resource():
            rec["amount"] = str(rec["amount"])
            rec["event_timestamp"] = rec["event_timestamp"].isoformat()
            records.append(rec)
        return records

    @task(task_id="validate_canonical_schema")
    def validate(records: list[dict[str, Any]]) -> dict[str, Any]:
        valid, errors = [], []
        for rec in records:
            try:
                rec = ensure_latest_version(rec)
                PaymentEventV2.model_validate(rec)
                valid.append(rec)
            except Exception as e:
                errors.append({"record": rec, "error": str(e)})
        return {"valid": valid, "errors": errors}

    @task(task_id="load_to_duckdb_and_parquet")
    def load(validated: dict[str, Any]) -> dict[str, int]:
        valid, errors = validated["valid"], validated["errors"]
        OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
        if errors:
            (OUTPUT_DIR / "errors.json").write_text(json.dumps(errors, indent=2, default=str))
        if valid:
            p = dlt.pipeline(
                pipeline_name="mal_payments",
                destination=dlt.destinations.duckdb(DUCKDB_PATH),
                dataset_name="payments_data",
            )
            p.run(valid, table_name="payment_events")
            con = duckdb.connect(DUCKDB_PATH)
            con.execute(
                f"COPY (SELECT * FROM payments_data.payment_events) "
                f"TO '{OUTPUT_DIR}/payment_events.parquet' (FORMAT PARQUET)"
            )
            con.close()
        return {"valid": len(valid), "errors": len(errors), "timestamp": datetime.now(timezone.utc).isoformat()}

    @task(task_id="run_analytics_queries")
    def run_analytics(load_stats: dict[str, int]) -> dict[str, Any]:
        con = duckdb.connect(DUCKDB_PATH, read_only=True)
        queries = [
            q.strip()
            for q in (SQL_DIR / "queries.sql").read_text().split(";")
            if q.strip() and not q.strip().startswith("--")
        ]
        results = {}
        for query in queries:
            label = [line for line in query.split("\n") if line.strip().startswith("--")]
            name = label[0].replace("--", "").strip() if label else query[:50]
            df = con.execute(query).fetchdf()
            results[name] = df.to_dict(orient="records")
            logger.info("Query [%s]: %d rows\n%s", name, len(df), df.to_string())
        con.close()
        report = {**load_stats, "query_count": len(results), "queries": list(results.keys())}
        (OUTPUT_DIR / "analytics_report.json").write_text(json.dumps(report, indent=2, default=str))
        return report

    raw = extract()
    checked = validate(raw)
    stats = load(checked)
    run_analytics(stats)


payment_pipeline()
