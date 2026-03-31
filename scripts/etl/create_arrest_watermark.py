#!/usr/bin/env python3
"""
Create and populate METADATA.ARREST_WATERMARK in Snowflake.

The watermark table tracks per-combination extraction state for the FBI CDE
arrest endpoints.  Each row represents a unique (FUNCTION, REPORT_GRANULARITY,
REPORT_LEVEL, OFFENSE_CODE) key and is used by downstream ETL scripts to know
what has been fetched and what still needs to be pulled.

Combinations inserted
---------------------
REPORT_GRANULARITY  REPORT_LEVEL source        Rows
------------------  ----------------------------  ----
NATIONAL            '<NONE>'  (literal)           1 × N offense codes
STATE               METADATA.STATES               51 × N offense codes
ORI                 METADATA.ORI                  M × N offense codes

Prerequisites
-------------
Run scripts/etl/create_fbi_metadata.py first to ensure
METADATA.STATES, METADATA.ORI, and METADATA.ARREST_OFFENSE_CODE exist.

Environment variables (read from .env or shell):
    SNOWFLAKE_ACCOUNT
    SNOWFLAKE_USER
    SNOWFLAKE_DATABASE
    SNOWFLAKE_SCHEMA
    SNOWFLAKE_WAREHOUSE
    SNOWFLAKE_PRIVATE_KEY_PATH      (default: snowflake_rsa_key.der)
"""

import logging
import os
import pathlib
import sys

import snowflake.connector
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization

# ---------------------------------------------------------------------------
# Bootstrap .env
# ---------------------------------------------------------------------------
_WORKSPACE_ROOT = pathlib.Path(__file__).resolve().parents[2]


def _load_dotenv(path: pathlib.Path) -> None:
    if not path.is_file():
        return
    with open(path, encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            key, _, val = line.partition("=")
            key = key.strip()
            val = val.strip().strip("'\"")
            if key and val:
                os.environ.setdefault(key, val)


_load_dotenv(_WORKSPACE_ROOT / ".env")

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Snowflake helpers
# ---------------------------------------------------------------------------

def _load_private_key_bytes() -> bytes:
    key_path = os.environ.get("SNOWFLAKE_PRIVATE_KEY_PATH", "snowflake_rsa_key.der")
    resolved = pathlib.Path(key_path)
    if not resolved.is_absolute():
        resolved = _WORKSPACE_ROOT / key_path
    if not resolved.is_file():
        log.error("Private key file not found: %s", resolved)
        sys.exit(1)
    key_path = str(resolved)
    with open(key_path, "rb") as fh:
        private_key = serialization.load_der_private_key(
            fh.read(),
            password=None,
            backend=default_backend(),
        )
    return private_key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )


def _build_snowflake_config() -> dict:
    required = ["SNOWFLAKE_ACCOUNT", "SNOWFLAKE_USER", "SNOWFLAKE_DATABASE"]
    missing = [v for v in required if not os.environ.get(v)]
    if missing:
        log.error("Missing required environment variables: %s", ", ".join(missing))
        sys.exit(1)

    return {
        "account":     os.environ["SNOWFLAKE_ACCOUNT"],
        "user":        os.environ["SNOWFLAKE_USER"],
        "private_key": _load_private_key_bytes(),
        "database":    os.environ["SNOWFLAKE_DATABASE"],
        "schema":      os.environ.get("SNOWFLAKE_SCHEMA", "PUBLIC"),
        "warehouse":   os.environ.get("SNOWFLAKE_WAREHOUSE", "FIN_TRADE_WH"),
    }


def _exec(cursor, sql: str, description: str) -> None:
    log.info("  %s", description)
    cursor.execute(sql)


# ---------------------------------------------------------------------------
# DDL
# ---------------------------------------------------------------------------

def create_arrest_watermark_table(cursor) -> None:
    _exec(
        cursor,
        """
        CREATE TABLE IF NOT EXISTS METADATA.ARREST_WATERMARK (
            FUNCTION                VARCHAR(20)   NOT NULL DEFAULT 'ARREST',
            REPORT_GRANULARITY      VARCHAR(10)   NOT NULL,
            REPORT_LEVEL            VARCHAR(20)   NOT NULL,
            OFFENSE_CODE            INTEGER       NOT NULL,
            FIRST_DATE              VARCHAR(7),
            LAST_DATE               VARCHAR(7),
            CONSECUTIVE_API_FAILURES INTEGER      NOT NULL DEFAULT 0,
            LAST_SUCCESSFUL_RUN     TIMESTAMP_TZ,
            LAST_ATTEMPTED_RUN      TIMESTAMP_TZ,
            CREATED_AT              TIMESTAMP_TZ  NOT NULL DEFAULT CURRENT_TIMESTAMP(),
            UPDATED_AT              TIMESTAMP_TZ  NOT NULL DEFAULT CURRENT_TIMESTAMP(),
            CONSTRAINT PK_ARREST_WATERMARK
                PRIMARY KEY (FUNCTION, REPORT_GRANULARITY, REPORT_LEVEL, OFFENSE_CODE)
        )
        """,
        "Create table METADATA.ARREST_WATERMARK",
    )


# ---------------------------------------------------------------------------
# DML — populate by granularity
# ---------------------------------------------------------------------------

def populate_national(cursor) -> None:
    _exec(
        cursor,
        """
        MERGE INTO METADATA.ARREST_WATERMARK AS tgt
        USING (
            SELECT
                'ARREST'   AS FUNCTION,
                'NATIONAL' AS REPORT_GRANULARITY,
                '<NONE>'   AS REPORT_LEVEL,
                OFFENSE_CODE
            FROM METADATA.ARREST_OFFENSE_CODE
        ) AS src
        ON  tgt.FUNCTION           = src.FUNCTION
        AND tgt.REPORT_GRANULARITY = src.REPORT_GRANULARITY
        AND tgt.REPORT_LEVEL       = src.REPORT_LEVEL
        AND tgt.OFFENSE_CODE       = src.OFFENSE_CODE
        WHEN NOT MATCHED THEN
            INSERT (FUNCTION, REPORT_GRANULARITY, REPORT_LEVEL, OFFENSE_CODE)
            VALUES (src.FUNCTION, src.REPORT_GRANULARITY, src.REPORT_LEVEL, src.OFFENSE_CODE)
        """,
        "Populate ARREST_WATERMARK — NATIONAL rows",
    )


def populate_state(cursor) -> None:
    _exec(
        cursor,
        """
        MERGE INTO METADATA.ARREST_WATERMARK AS tgt
        USING (
            SELECT
                'ARREST' AS FUNCTION,
                'STATE'  AS REPORT_GRANULARITY,
                s.STATE  AS REPORT_LEVEL,
                oc.OFFENSE_CODE
            FROM METADATA.STATES              s
            CROSS JOIN METADATA.ARREST_OFFENSE_CODE oc
        ) AS src
        ON  tgt.FUNCTION           = src.FUNCTION
        AND tgt.REPORT_GRANULARITY = src.REPORT_GRANULARITY
        AND tgt.REPORT_LEVEL       = src.REPORT_LEVEL
        AND tgt.OFFENSE_CODE       = src.OFFENSE_CODE
        WHEN NOT MATCHED THEN
            INSERT (FUNCTION, REPORT_GRANULARITY, REPORT_LEVEL, OFFENSE_CODE)
            VALUES (src.FUNCTION, src.REPORT_GRANULARITY, src.REPORT_LEVEL, src.OFFENSE_CODE)
        """,
        "Populate ARREST_WATERMARK — STATE rows (METADATA.STATES × METADATA.ARREST_OFFENSE_CODE)",
    )


def populate_ori(cursor) -> None:
    _exec(
        cursor,
        """
        MERGE INTO METADATA.ARREST_WATERMARK AS tgt
        USING (
            SELECT
                'ARREST' AS FUNCTION,
                'ORI'    AS REPORT_GRANULARITY,
                o.ORI    AS REPORT_LEVEL,
                oc.OFFENSE_CODE
            FROM METADATA.ORI                 o
            CROSS JOIN METADATA.ARREST_OFFENSE_CODE oc
        ) AS src
        ON  tgt.FUNCTION           = src.FUNCTION
        AND tgt.REPORT_GRANULARITY = src.REPORT_GRANULARITY
        AND tgt.REPORT_LEVEL       = src.REPORT_LEVEL
        AND tgt.OFFENSE_CODE       = src.OFFENSE_CODE
        WHEN NOT MATCHED THEN
            INSERT (FUNCTION, REPORT_GRANULARITY, REPORT_LEVEL, OFFENSE_CODE)
            VALUES (src.FUNCTION, src.REPORT_GRANULARITY, src.REPORT_LEVEL, src.OFFENSE_CODE)
        """,
        "Populate ARREST_WATERMARK — ORI rows (METADATA.ORI × METADATA.ARREST_OFFENSE_CODE)",
    )


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    log.info("Arrest watermark setup — connecting to Snowflake …")
    config = _build_snowflake_config()

    conn = snowflake.connector.connect(**config)
    try:
        cursor = conn.cursor()
        try:
            create_arrest_watermark_table(cursor)
            populate_national(cursor)
            populate_state(cursor)
            populate_ori(cursor)
            conn.commit()
            log.info("Done — METADATA.ARREST_WATERMARK created and populated.")
        finally:
            cursor.close()
    finally:
        conn.close()


if __name__ == "__main__":
    main()
