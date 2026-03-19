#!/usr/bin/env python3
"""
SEC Filings EDA Explorer

Connects to the SEC_FILINGS.CYBERSYN Snowflake schema and exports an Excel
workbook to eda/data/sec_filings_eda.xlsx containing:
  - Listing  : table metadata (name, last updated, row count)
  - <TableN> : random sample of up to 1,000 rows for each table

Reads credentials from a .env file in the workspace root (or environment).
Required variables:
  SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, SNOWFLAKE_WAREHOUSE
  SNOWFLAKE_PRIVATE_KEY_PATH  (default: snowflake_rsa_key.der)
  SNOWFLAKE_ROLE              (optional)
"""

import logging
import os
import re
import sys
from pathlib import Path

from dotenv import load_dotenv
import pandas as pd
import snowflake.connector
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization

# Load .env from workspace root (no-op if file absent or vars already set)
_WORKSPACE_ROOT = Path(__file__).resolve().parents[2]
load_dotenv(_WORKSPACE_ROOT / ".env")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

DATABASE = "SEC_FILINGS"
SCHEMA = "CYBERSYN"
SAMPLE_ROWS = 1000

# Resolves to <workspace_root>/eda/data/sec_filings_eda.xlsx
OUTPUT_PATH = _WORKSPACE_ROOT / "eda" / "data" / "sec_filings_eda.xlsx"


def load_private_key_bytes() -> bytes:
    key_path = os.environ.get("SNOWFLAKE_PRIVATE_KEY_PATH", "snowflake_rsa_key.der")
    if not os.path.isfile(key_path):
        raise FileNotFoundError(f"Snowflake private key not found: {key_path}")

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


def get_connection() -> snowflake.connector.SnowflakeConnection:
    conn_params = {
        "account": os.environ["SNOWFLAKE_ACCOUNT"],
        "user": os.environ["SNOWFLAKE_USER"],
        "private_key": load_private_key_bytes(),
        "database": DATABASE,
        "schema": SCHEMA,
        "warehouse": os.environ["SNOWFLAKE_WAREHOUSE"],
    }
    role = os.environ.get("SNOWFLAKE_ROLE")
    if role:
        conn_params["role"] = role

    logger.info("Connecting to Snowflake (%s.%s)...", DATABASE, SCHEMA)
    return snowflake.connector.connect(**conn_params)


def safe_sheet_name(name: str, used: set) -> str:
    """Truncate to Excel's 31-character limit and deduplicate sheet names."""
    truncated = name[:31]
    candidate = truncated
    counter = 1
    while candidate in used:
        suffix = f"_{counter}"
        candidate = truncated[: 31 - len(suffix)] + suffix
        counter += 1
    used.add(candidate)
    return candidate


def fetch_table_listing(conn: snowflake.connector.SnowflakeConnection) -> pd.DataFrame:
    # Cybersyn Marketplace data is exposed as views, not base tables.
    logger.info("Fetching view listing via SHOW VIEWS IN SCHEMA %s.%s...", DATABASE, SCHEMA)
    cursor = conn.cursor()
    cursor.execute(f"SHOW VIEWS IN SCHEMA {DATABASE}.{SCHEMA}")
    cursor.execute('SELECT "name", "created_on" FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))')
    rows = cursor.fetchall()
    col_names = [desc[0] for desc in cursor.description]
    cursor.close()

    df = pd.DataFrame(rows, columns=col_names)
    df = df.rename(columns={"name": "TABLE_NAME", "created_on": "LAST_UPDATED"})
    # Excel doesn't support tz-aware datetimes — strip timezone info
    if pd.api.types.is_datetime64_any_dtype(df["LAST_UPDATED"]):
        df["LAST_UPDATED"] = df["LAST_UPDATED"].dt.tz_localize(None)
    df = df.sort_values("TABLE_NAME").reset_index(drop=True)
    logger.info("Found %d views", len(df))
    return df


def fetch_table_sample(
    conn: snowflake.connector.SnowflakeConnection, table_name: str
) -> pd.DataFrame:
    # Views don't support block sampling; use LIMIT with ORDER BY RANDOM() on a
    # subquery capped at 100k rows so the shuffle stays fast on large views.
    sql = f"""
        SELECT * FROM (
            SELECT * FROM {DATABASE}.{SCHEMA}."{table_name}" LIMIT 100000
        ) ORDER BY RANDOM() LIMIT {SAMPLE_ROWS}
    """
    logger.info("Sampling up to %d rows from %s...", SAMPLE_ROWS, table_name)
    cursor = conn.cursor()
    cursor.execute(sql)
    df = cursor.fetch_pandas_all()
    cursor.close()
    # Strip timezone from any datetime columns so Excel can accept them
    for col in df.select_dtypes(include=["datetimetz"]).columns:
        df[col] = df[col].dt.tz_localize(None)
    # Remove illegal XML/Excel characters (control chars except tab/newline) from string columns
    _illegal_xml = re.compile(r"[\x00-\x08\x0b\x0c\x0e-\x1f]")
    for col in df.select_dtypes(include=["object"]).columns:
        df[col] = df[col].apply(
            lambda v: _illegal_xml.sub("", v) if isinstance(v, str) else v
        )
    return df


def main() -> int:
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)

    try:
        conn = get_connection()
    except KeyError as exc:
        logger.error("Missing required environment variable: %s", exc)
        return 1
    except FileNotFoundError as exc:
        logger.error("%s", exc)
        return 1

    try:
        listing_df = fetch_table_listing(conn)
        table_names: list = listing_df["TABLE_NAME"].tolist()
        logger.info("Processing %d views in %s.%s", len(table_names), DATABASE, SCHEMA)

        used_sheet_names: set = {"Listing"}

        with pd.ExcelWriter(OUTPUT_PATH, engine="openpyxl") as writer:
            listing_df.to_excel(writer, sheet_name="Listing", index=False)
            logger.info("Wrote 'Listing' tab (%d tables)", len(listing_df))

            for table_name in table_names:
                try:
                    sample_df = fetch_table_sample(conn, table_name)
                    sheet_name = safe_sheet_name(table_name, used_sheet_names)
                    sample_df.to_excel(writer, sheet_name=sheet_name, index=False)
                    logger.info(
                        "Wrote tab '%s' (%d rows, %d columns)",
                        sheet_name,
                        len(sample_df),
                        len(sample_df.columns),
                    )
                except Exception as exc:
                    logger.warning("Skipping table '%s': %s", table_name, exc)

    finally:
        conn.close()
        logger.info("Snowflake connection closed")

    logger.info("Output written to %s", OUTPUT_PATH)
    return 0


if __name__ == "__main__":
    sys.exit(main())
