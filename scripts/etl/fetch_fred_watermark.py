#!/usr/bin/env python3
"""
FRED watermark-based ETL.

This script:
1. Ensures Snowflake watermark/log tables exist.
2. Loads configured FRED series from YAML.
3. Fetches only new observations since each series watermark.
4. Uploads CSV files to S3.
5. Updates series watermarks and extraction log records in Snowflake.
"""

import csv
import json
import logging
import os
import re
import sys
from datetime import datetime, timedelta, timezone
from io import StringIO
from typing import Dict, List, Optional, Tuple

import boto3
import snowflake.connector
import yaml
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization
from fredapi import Fred


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


class FredWatermarkManager:
    """Manages watermark and extraction log state in Snowflake."""

    def __init__(self, snowflake_config: Dict[str, object]):
        self.snowflake_config = snowflake_config
        self.connection = None

    def connect(self) -> None:
        if not self.connection:
            self.connection = snowflake.connector.connect(**self.snowflake_config)
            logger.info("Connected to Snowflake")

    def close(self) -> None:
        if self.connection:
            self.connection.close()
            self.connection = None
            logger.info("Snowflake connection closed")

    def ensure_tables(self) -> None:
        self.connect()
        cursor = self.connection.cursor()
        try:
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS FRED_SERIES_WATERMARKS (
                    SERIES_ID VARCHAR(100) PRIMARY KEY,
                    TITLE VARCHAR,
                    FREQUENCY VARCHAR(100),
                    UNITS VARCHAR(100),
                    LAST_OBSERVATION_DATE DATE,
                    LAST_SUCCESSFUL_RUN TIMESTAMP_TZ,
                    CONSECUTIVE_FAILURES NUMBER(10, 0) DEFAULT 0,
                    UPDATED_AT TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP()
                )
                """
            )
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS FRED_EXTRACTION_LOG (
                    RUN_ID VARCHAR(100),
                    SERIES_ID VARCHAR(100),
                    EXTRACTED_AT TIMESTAMP_TZ,
                    STATUS VARCHAR(20),
                    RECORD_COUNT NUMBER(10, 0),
                    MESSAGE VARCHAR,
                    S3_KEY VARCHAR,
                    START_DATE DATE,
                    END_DATE DATE
                )
                """
            )
            self.connection.commit()
            logger.info("Ensured Snowflake FRED tables exist")
        finally:
            cursor.close()

    def get_existing_watermarks(self, series_ids: List[str]) -> Dict[str, Optional[str]]:
        if not series_ids:
            return {}

        self.connect()
        cursor = self.connection.cursor()
        try:
            escaped_ids = [sid.replace("'", "''") for sid in series_ids]
            in_clause = "', '".join(escaped_ids)
            query = f"""
                SELECT SERIES_ID, TO_CHAR(LAST_OBSERVATION_DATE, 'YYYY-MM-DD')
                FROM FRED_SERIES_WATERMARKS
                WHERE SERIES_ID IN ('{in_clause}')
            """
            cursor.execute(query)
            rows = cursor.fetchall()
            return {row[0]: row[1] for row in rows}
        finally:
            cursor.close()

    def bulk_update_watermarks(self, successful_updates: List[Dict[str, str]]) -> None:
        if not successful_updates:
            return

        self.connect()
        cursor = self.connection.cursor()
        try:
            cursor.execute(
                """
                CREATE TEMPORARY TABLE TMP_FRED_WATERMARK_UPDATES (
                    SERIES_ID VARCHAR(100),
                    TITLE VARCHAR,
                    FREQUENCY VARCHAR(100),
                    UNITS VARCHAR(100),
                    LAST_OBSERVATION_DATE DATE
                )
                """
            )

            values_sql = []
            for item in successful_updates:
                series_id = item["series_id"].replace("'", "''")
                title = (item.get("title") or "").replace("'", "''")
                frequency = (item.get("frequency") or "").replace("'", "''")
                units = (item.get("units") or "").replace("'", "''")
                last_obs = item["last_observation_date"].replace("'", "''")
                values_sql.append(
                    "(" +
                    f"'{series_id}', '{title}', '{frequency}', '{units}', TO_DATE('{last_obs}', 'YYYY-MM-DD')" +
                    ")"
                )

            cursor.execute(
                """
                INSERT INTO TMP_FRED_WATERMARK_UPDATES (
                    SERIES_ID,
                    TITLE,
                    FREQUENCY,
                    UNITS,
                    LAST_OBSERVATION_DATE
                )
                VALUES
                """
                + ",\n".join(values_sql)
            )

            cursor.execute(
                """
                MERGE INTO FRED_SERIES_WATERMARKS target
                USING TMP_FRED_WATERMARK_UPDATES source
                ON target.SERIES_ID = source.SERIES_ID
                WHEN MATCHED THEN UPDATE SET
                    TITLE = source.TITLE,
                    FREQUENCY = source.FREQUENCY,
                    UNITS = source.UNITS,
                    LAST_OBSERVATION_DATE = source.LAST_OBSERVATION_DATE,
                    LAST_SUCCESSFUL_RUN = CURRENT_TIMESTAMP(),
                    CONSECUTIVE_FAILURES = 0,
                    UPDATED_AT = CURRENT_TIMESTAMP()
                WHEN NOT MATCHED THEN INSERT (
                    SERIES_ID,
                    TITLE,
                    FREQUENCY,
                    UNITS,
                    LAST_OBSERVATION_DATE,
                    LAST_SUCCESSFUL_RUN,
                    CONSECUTIVE_FAILURES,
                    UPDATED_AT
                ) VALUES (
                    source.SERIES_ID,
                    source.TITLE,
                    source.FREQUENCY,
                    source.UNITS,
                    source.LAST_OBSERVATION_DATE,
                    CURRENT_TIMESTAMP(),
                    0,
                    CURRENT_TIMESTAMP()
                )
                """
            )

            self.connection.commit()
            logger.info("Updated %d watermarks", len(successful_updates))
        finally:
            cursor.close()

    def increment_failures(self, failed_series_ids: List[str]) -> None:
        if not failed_series_ids:
            return

        self.connect()
        cursor = self.connection.cursor()
        try:
            escaped_ids = [sid.replace("'", "''") for sid in failed_series_ids]
            in_clause = "', '".join(escaped_ids)
            cursor.execute(
                f"""
                UPDATE FRED_SERIES_WATERMARKS
                SET
                    CONSECUTIVE_FAILURES = COALESCE(CONSECUTIVE_FAILURES, 0) + 1,
                    UPDATED_AT = CURRENT_TIMESTAMP()
                WHERE SERIES_ID IN ('{in_clause}')
                """
            )
            self.connection.commit()
            logger.info("Incremented failures for %d series", len(failed_series_ids))
        finally:
            cursor.close()

    def insert_extraction_logs(self, logs: List[Dict[str, object]]) -> None:
        if not logs:
            return

        self.connect()
        cursor = self.connection.cursor()
        try:
            insert_sql = """
                INSERT INTO FRED_EXTRACTION_LOG (
                    RUN_ID,
                    SERIES_ID,
                    EXTRACTED_AT,
                    STATUS,
                    RECORD_COUNT,
                    MESSAGE,
                    S3_KEY,
                    START_DATE,
                    END_DATE
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            rows = [
                (
                    item["run_id"],
                    item["series_id"],
                    item["extracted_at"],
                    item["status"],
                    item.get("record_count", 0),
                    item.get("message"),
                    item.get("s3_key"),
                    item.get("start_date"),
                    item.get("end_date"),
                )
                for item in logs
            ]
            cursor.executemany(insert_sql, rows)
            self.connection.commit()
            logger.info("Inserted %d extraction log rows", len(logs))
        finally:
            cursor.close()


def load_private_key_bytes() -> bytes:
    key_path = os.environ.get("SNOWFLAKE_PRIVATE_KEY_PATH", "snowflake_rsa_key.der")
    if not os.path.isfile(key_path):
        logger.error("Private key file not found: %s", key_path)
        sys.exit(1)

    with open(key_path, "rb") as key_file:
        private_key = serialization.load_der_private_key(
            key_file.read(),
            password=None,
            backend=default_backend(),
        )

    return private_key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )


def load_series_config(
    config_path: str,
    category_filter: Optional[str] = None,
) -> Tuple[str, List[str]]:
    with open(config_path, "r", encoding="utf-8") as f:
        config = yaml.safe_load(f) or {}

    config_start_date = str(config.get("start_date", "1950-01-01"))
    common_series = config.get("common_series", {})
    all_series: List[str] = []

    for category, series_list in common_series.items():
        if category_filter and category.lower() != category_filter.lower():
            continue
        for item in series_list:
            all_series.append(str(item).split()[0])

    # Preserve order while removing duplicates.
    deduped_series = list(dict.fromkeys(all_series))
    return config_start_date, deduped_series


def get_start_date(existing_last_observation: Optional[str], default_start_date: str) -> str:
    if not existing_last_observation:
        return default_start_date

    next_day = datetime.strptime(existing_last_observation, "%Y-%m-%d") + timedelta(days=1)
    return next_day.strftime("%Y-%m-%d")


def fetch_series_delta(
    fred: Fred,
    series_id: str,
    start_date: str,
) -> Tuple[List[Dict[str, object]], Dict[str, str]]:
    metadata = fred.get_series_info(series_id)
    series = fred.get_series(series_id, observation_start=start_date)

    rows: List[Dict[str, object]] = []
    for observation_date, value in series.items():
        if value is None:
            continue
        if hasattr(value, "item"):
            value = value.item()

        rows.append(
            {
                "series_id": series_id,
                "date": observation_date.date().isoformat(),
                "value": float(value),
            }
        )

    info = {
        "title": str(getattr(metadata, "title", "") or ""),
        "frequency": str(getattr(metadata, "frequency", "") or ""),
        "units": str(getattr(metadata, "units", "") or ""),
    }

    return rows, info


def upload_rows_to_s3(
    s3_client,
    bucket: str,
    prefix: str,
    series_id: str,
    rows: List[Dict[str, object]],
    meta: Dict[str, str],
    extracted_at_iso: str,
) -> str:
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    s3_key = f"{prefix}series_id={series_id}/{series_id}_{timestamp}.csv"

    buffer = StringIO()
    writer = csv.DictWriter(
        buffer,
        fieldnames=[
            "series_id",
            "date",
            "value",
            "title",
            "frequency",
            "units",
            "extracted_at",
        ],
    )
    writer.writeheader()

    for row in rows:
        writer.writerow(
            {
                "series_id": row["series_id"],
                "date": row["date"],
                "value": row["value"],
                "title": meta.get("title", ""),
                "frequency": meta.get("frequency", ""),
                "units": meta.get("units", ""),
                "extracted_at": extracted_at_iso,
            }
        )

    s3_client.put_object(
        Bucket=bucket,
        Key=s3_key,
        Body=buffer.getvalue().encode("utf-8"),
        ContentType="text/csv",
    )
    return s3_key


def upload_rows_to_local(
    output_dir: str,
    series_id: str,
    rows: List[Dict[str, object]],
    meta: Dict[str, str],
    extracted_at_iso: str,
) -> str:
    safe_series_id = re.sub(r"[^A-Za-z0-9_-]", "_", series_id)
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    series_dir = os.path.join(output_dir, f"series_id={safe_series_id}")
    os.makedirs(series_dir, exist_ok=True)
    output_path = os.path.join(series_dir, f"{safe_series_id}_{timestamp}.csv")

    with open(output_path, "w", encoding="utf-8", newline="") as csvfile:
        writer = csv.DictWriter(
            csvfile,
            fieldnames=[
                "series_id",
                "date",
                "value",
                "title",
                "frequency",
                "units",
                "extracted_at",
            ],
        )
        writer.writeheader()
        for row in rows:
            writer.writerow(
                {
                    "series_id": row["series_id"],
                    "date": row["date"],
                    "value": row["value"],
                    "title": meta.get("title", ""),
                    "frequency": meta.get("frequency", ""),
                    "units": meta.get("units", ""),
                    "extracted_at": extracted_at_iso,
                }
            )

    return output_path


def build_snowflake_config() -> Dict[str, object]:
    return {
        "account": os.environ["SNOWFLAKE_ACCOUNT"],
        "user": os.environ["SNOWFLAKE_USER"],
        "private_key": load_private_key_bytes(),
        "database": os.environ["SNOWFLAKE_DATABASE"],
        "schema": os.environ["SNOWFLAKE_SCHEMA"],
        "warehouse": os.environ["SNOWFLAKE_WAREHOUSE"],
    }


def main() -> None:
    logger.info("Starting FRED watermark ETL")

    fred_api_key = os.environ["FRED_API_KEY"]
    disable_watermark = os.environ.get("FRED_DISABLE_WATERMARK", "false").lower() == "true"
    upload_target = os.environ.get("FRED_UPLOAD_TARGET", "s3").lower()
    local_output_dir = os.environ.get("FRED_LOCAL_OUTPUT_DIR", "/tmp/fred_data")
    s3_bucket = os.environ.get("S3_BUCKET", "fin-trade-extract-nosubs-bucket")
    s3_prefix = os.environ.get("S3_FRED_PREFIX", "fred/")
    if not s3_prefix.endswith("/"):
        s3_prefix += "/"

    config_path = os.environ.get("FRED_CONFIG_PATH", "config/fred_series.yml")
    default_start_date_override = os.environ.get("FRED_DEFAULT_START_DATE")
    category_filter = os.environ.get("CATEGORY_FILTER")
    max_series = os.environ.get("MAX_SERIES")
    max_series_int = int(max_series) if max_series else None

    config_start_date, series_ids = load_series_config(
        config_path=config_path,
        category_filter=category_filter,
    )
    default_start_date = default_start_date_override or config_start_date
    if max_series_int is not None:
        series_ids = series_ids[:max_series_int]

    if not series_ids:
        logger.warning("No configured FRED series found to process")
        return

    logger.info("Prepared %d FRED series for processing", len(series_ids))

    run_id = datetime.now(timezone.utc).strftime("fred_%Y%m%d_%H%M%S")
    extracted_at_iso = datetime.now(timezone.utc).isoformat()

    existing_watermarks: Dict[str, Optional[str]] = {}
    snowflake_config: Optional[Dict[str, object]] = None
    if not disable_watermark:
        snowflake_config = build_snowflake_config()
        watermark_manager = FredWatermarkManager(snowflake_config)

        try:
            watermark_manager.ensure_tables()
            existing_watermarks = watermark_manager.get_existing_watermarks(series_ids)
        finally:
            watermark_manager.close()
    else:
        logger.info("Watermark mode disabled; using default start date for all series")

    fred = Fred(api_key=fred_api_key)
    s3_client = boto3.client("s3") if upload_target == "s3" else None
    if upload_target == "local":
        os.makedirs(local_output_dir, exist_ok=True)
    elif upload_target != "s3":
        logger.error("Invalid FRED_UPLOAD_TARGET '%s'. Use 's3' or 'local'.", upload_target)
        sys.exit(1)

    successful_updates: List[Dict[str, str]] = []
    failed_series_ids: List[str] = []
    extraction_logs: List[Dict[str, object]] = []

    totals = {
        "total_series": len(series_ids),
        "successful_series": 0,
        "failed_series": 0,
        "rows_uploaded": 0,
    }

    for idx, series_id in enumerate(series_ids, start=1):
        start_date = get_start_date(existing_watermarks.get(series_id), default_start_date)
        logger.info("[%d/%d] Processing %s from %s", idx, len(series_ids), series_id, start_date)

        try:
            rows, meta = fetch_series_delta(fred, series_id, start_date)
            if not rows:
                extraction_logs.append(
                    {
                        "run_id": run_id,
                        "series_id": series_id,
                        "extracted_at": extracted_at_iso,
                        "status": "success",
                        "record_count": 0,
                        "message": "No new observations",
                        "s3_key": None,
                        "start_date": start_date,
                        "end_date": None,
                    }
                )
                totals["successful_series"] += 1
                logger.info("No new observations for %s", series_id)
                continue

            if upload_target == "s3":
                s3_key = upload_rows_to_s3(
                    s3_client=s3_client,
                    bucket=s3_bucket,
                    prefix=s3_prefix,
                    series_id=series_id,
                    rows=rows,
                    meta=meta,
                    extracted_at_iso=extracted_at_iso,
                )
            else:
                s3_key = upload_rows_to_local(
                    output_dir=local_output_dir,
                    series_id=series_id,
                    rows=rows,
                    meta=meta,
                    extracted_at_iso=extracted_at_iso,
                )

            last_observation_date = rows[-1]["date"]
            successful_updates.append(
                {
                    "series_id": series_id,
                    "title": meta.get("title", ""),
                    "frequency": meta.get("frequency", ""),
                    "units": meta.get("units", ""),
                    "last_observation_date": str(last_observation_date),
                }
            )
            extraction_logs.append(
                {
                    "run_id": run_id,
                    "series_id": series_id,
                    "extracted_at": extracted_at_iso,
                    "status": "success",
                    "record_count": len(rows),
                    "message": "Uploaded delta observations",
                    "s3_key": s3_key,
                    "start_date": start_date,
                    "end_date": last_observation_date,
                }
            )

            totals["successful_series"] += 1
            totals["rows_uploaded"] += len(rows)
            if upload_target == "s3":
                logger.info("Uploaded %d rows for %s to s3://%s/%s", len(rows), series_id, s3_bucket, s3_key)
            else:
                logger.info("Wrote %d rows for %s to %s", len(rows), series_id, s3_key)

        except Exception as exc:
            failed_series_ids.append(series_id)
            totals["failed_series"] += 1
            logger.exception("Failed processing %s", series_id)
            extraction_logs.append(
                {
                    "run_id": run_id,
                    "series_id": series_id,
                    "extracted_at": extracted_at_iso,
                    "status": "error",
                    "record_count": 0,
                    "message": str(exc),
                    "s3_key": None,
                    "start_date": start_date,
                    "end_date": None,
                }
            )

    if not disable_watermark and snowflake_config is not None:
        watermark_manager = FredWatermarkManager(snowflake_config)
        try:
            watermark_manager.connect()
            watermark_manager.bulk_update_watermarks(successful_updates)
            watermark_manager.increment_failures(failed_series_ids)
            watermark_manager.insert_extraction_logs(extraction_logs)
        finally:
            watermark_manager.close()

    results = {
        "run_id": run_id,
        "started_at": extracted_at_iso,
        "finished_at": datetime.now(timezone.utc).isoformat(),
        "s3_bucket": s3_bucket,
        "s3_prefix": s3_prefix,
        "upload_target": upload_target,
        "disable_watermark": disable_watermark,
        "local_output_dir": local_output_dir if upload_target == "local" else None,
        "default_start_date": default_start_date,
        "category_filter": category_filter,
        "max_series": max_series_int,
        **totals,
    }

    with open("/tmp/fred_watermark_results.json", "w", encoding="utf-8") as f:
        json.dump(results, f, indent=2)

    logger.info("FRED ETL complete: %s", json.dumps(results))


if __name__ == "__main__":
    main()
