#!/usr/bin/env python3
"""
Create and populate FBI metadata tables in Snowflake under the METADATA schema.

Tables created / populated:
    METADATA.STATES                 — all 50 US state abbreviations + DC
    METADATA.ORI                    — unique ORI values from CURATED_FBI.FBI_AGENCIES
    METADATA.ARREST_OFFENSE_CODE    — FBI CDE arrest endpoint offense codes
    METADATA.SUPPLEMENTAL_OFFENSE_CODE — FBI SRS supplemental offense codes

Also creates:
    METADATA.SEQ_STATES             — sequence for surrogate key on STATES

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
# Reference data
# ---------------------------------------------------------------------------
ALL_STATES = [
    "AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "FL", "GA",
    "HI", "ID", "IL", "IN", "IA", "KS", "KY", "LA", "ME", "MD",
    "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH", "NJ",
    "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI", "SC",
    "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV", "WI", "WY",
    "DC",
]

ARREST_OFFENSE_CODES = [
    310, 110,  50,  60, 101, 330, 290, 260, 150, 158, 157, 160, 159, 156, 153,
    152, 155, 154, 151, 280, 200, 180, 190, 173, 171, 172, 170, 102,  70, 270,
     12,  90,  11, 250, 140, 142, 141, 143,  23,  20,  30, 240,  55, 210, 300,
    220, 230,
]

SUPPLEMENTAL_OFFENSE_CODES = ["NB", "NV", "NMVT", "NROB"]


# ---------------------------------------------------------------------------
# Snowflake helpers
# ---------------------------------------------------------------------------

def _load_private_key_bytes() -> bytes:
    key_path = os.environ.get("SNOWFLAKE_PRIVATE_KEY_PATH", "snowflake_rsa_key.der")
    if not os.path.isfile(key_path):
        log.error("Private key file not found: %s", key_path)
        sys.exit(1)
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
# DDL + DML
# ---------------------------------------------------------------------------

def create_metadata_schema(cursor) -> None:
    _exec(cursor, "CREATE SCHEMA IF NOT EXISTS METADATA", "Ensure METADATA schema exists")


def create_and_populate_states(cursor) -> None:
    _exec(
        cursor,
        """
        CREATE SEQUENCE IF NOT EXISTS METADATA.SEQ_STATES
            START = 1
            INCREMENT = 1
            COMMENT = 'Surrogate key sequence for METADATA.STATES'
        """,
        "Create sequence METADATA.SEQ_STATES",
    )

    _exec(
        cursor,
        """
        CREATE TABLE IF NOT EXISTS METADATA.STATES (
            STATE_ID   INTEGER       DEFAULT METADATA.SEQ_STATES.NEXTVAL,
            STATE      VARCHAR(5)    NOT NULL,
            CREATED_AT TIMESTAMP_TZ  DEFAULT CURRENT_TIMESTAMP(),
            CONSTRAINT PK_STATES PRIMARY KEY (STATE)
        )
        """,
        "Create table METADATA.STATES",
    )

    values_sql = ",\n            ".join(f"('{s}')" for s in ALL_STATES)
    _exec(
        cursor,
        f"""
        MERGE INTO METADATA.STATES AS tgt
        USING (
            SELECT column1 AS STATE
            FROM VALUES
            {values_sql}
        ) AS src ON tgt.STATE = src.STATE
        WHEN NOT MATCHED THEN
            INSERT (STATE) VALUES (src.STATE)
        """,
        f"Populate METADATA.STATES ({len(ALL_STATES)} states)",
    )


def create_and_populate_arrest_offense_codes(cursor) -> None:
    _exec(
        cursor,
        """
        CREATE TABLE IF NOT EXISTS METADATA.ARREST_OFFENSE_CODE (
            OFFENSE_CODE INTEGER      NOT NULL,
            CREATED_AT   TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP(),
            CONSTRAINT PK_ARREST_OFFENSE_CODE PRIMARY KEY (OFFENSE_CODE)
        )
        """,
        "Create table METADATA.ARREST_OFFENSE_CODE",
    )

    values_sql = ",\n            ".join(f"({c})" for c in ARREST_OFFENSE_CODES)
    _exec(
        cursor,
        f"""
        MERGE INTO METADATA.ARREST_OFFENSE_CODE AS tgt
        USING (
            SELECT column1 AS OFFENSE_CODE
            FROM VALUES
            {values_sql}
        ) AS src ON tgt.OFFENSE_CODE = src.OFFENSE_CODE
        WHEN NOT MATCHED THEN
            INSERT (OFFENSE_CODE) VALUES (src.OFFENSE_CODE)
        """,
        f"Populate METADATA.ARREST_OFFENSE_CODE ({len(ARREST_OFFENSE_CODES)} codes)",
    )


def create_and_populate_supplemental_offense_codes(cursor) -> None:
    _exec(
        cursor,
        """
        CREATE TABLE IF NOT EXISTS METADATA.SUPPLEMENTAL_OFFENSE_CODE (
            OFFENSE_CODE VARCHAR(10)  NOT NULL,
            CREATED_AT   TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP(),
            CONSTRAINT PK_SUPPLEMENTAL_OFFENSE_CODE PRIMARY KEY (OFFENSE_CODE)
        )
        """,
        "Create table METADATA.SUPPLEMENTAL_OFFENSE_CODE",
    )

    values_sql = ",\n            ".join(f"('{c}')" for c in SUPPLEMENTAL_OFFENSE_CODES)
    _exec(
        cursor,
        f"""
        MERGE INTO METADATA.SUPPLEMENTAL_OFFENSE_CODE AS tgt
        USING (
            SELECT column1 AS OFFENSE_CODE
            FROM VALUES
            {values_sql}
        ) AS src ON tgt.OFFENSE_CODE = src.OFFENSE_CODE
        WHEN NOT MATCHED THEN
            INSERT (OFFENSE_CODE) VALUES (src.OFFENSE_CODE)
        """,
        f"Populate METADATA.SUPPLEMENTAL_OFFENSE_CODE ({len(SUPPLEMENTAL_OFFENSE_CODES)} codes)",
    )


def create_and_populate_ori(cursor) -> None:
    _exec(
        cursor,
        """
        CREATE TABLE IF NOT EXISTS METADATA.ORI (
            ORI        VARCHAR(20)  NOT NULL,
            CREATED_AT TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP(),
            UPDATED_AT TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP(),
            CONSTRAINT PK_ORI PRIMARY KEY (ORI)
        )
        """,
        "Create table METADATA.ORI",
    )

    _exec(
        cursor,
        """
        MERGE INTO METADATA.ORI AS tgt
        USING (
            SELECT DISTINCT ORI
            FROM CURATED_FBI.FBI_AGENCIES
            WHERE ORI IS NOT NULL
        ) AS src ON tgt.ORI = src.ORI
        WHEN NOT MATCHED THEN
            INSERT (ORI) VALUES (src.ORI)
        WHEN MATCHED THEN
            UPDATE SET tgt.UPDATED_AT = CURRENT_TIMESTAMP()
        """,
        "Populate METADATA.ORI from CURATED_FBI.FBI_AGENCIES",
    )


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    log.info("FBI Metadata table setup — connecting to Snowflake …")
    config = _build_snowflake_config()

    conn = snowflake.connector.connect(**config)
    try:
        cursor = conn.cursor()
        try:
            log.info("Creating METADATA schema and tables …")
            create_metadata_schema(cursor)
            create_and_populate_states(cursor)
            create_and_populate_arrest_offense_codes(cursor)
            create_and_populate_supplemental_offense_codes(cursor)
            create_and_populate_ori(cursor)
            conn.commit()
            log.info("Done — all METADATA tables created and populated.")
        finally:
            cursor.close()
    finally:
        conn.close()


if __name__ == "__main__":
    main()
