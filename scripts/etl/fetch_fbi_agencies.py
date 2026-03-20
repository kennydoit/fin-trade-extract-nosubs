#!/usr/bin/env python3
"""
FBI CDE Agency ETL — fetch all agencies by state and upload to S3 as NDJSON.

For each US state abbreviation, calls:
    GET https://api.usa.gov/crime/fbi/cde/agency/byStateAbbr/{state}?API_KEY=…

The API returns {county_name: [agency, ...]} dicts. This script flattens all
agencies across counties into one NDJSON file per state and uploads to:
    s3://{S3_BUCKET}/{S3_FBI_PREFIX}agencies/state={STATE}/{STATE}_{timestamp}.ndjson

Environment variables (all read from .env or shell):
    DATA_GOV_API_KEY        – FBI CDE / data.gov API key
    S3_BUCKET               – target S3 bucket name
    S3_FBI_PREFIX           – optional prefix inside bucket (default: fbi/)
    AWS_ACCESS_KEY_ID       – AWS credentials
    AWS_SECRET_ACCESS_KEY   – AWS credentials
    AWS_REGION              – AWS region (default: us-east-1)
"""

import json
import logging
import os
import pathlib
import sys
import time
from datetime import datetime, timezone
from typing import Any

import boto3
import requests

# ---------------------------------------------------------------------------
# Bootstrap .env  (same approach as other scripts in this repo)
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
# Config
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

BASE_URL    = "https://api.usa.gov/crime/fbi/cde"
API_KEY     = os.environ.get("DATA_GOV_API_KEY", "")
S3_BUCKET   = os.environ.get("S3_BUCKET", "fin-trade-extract-nosubs-bucket")
S3_PREFIX   = os.environ.get("S3_FBI_PREFIX", "fbi/")
if not S3_PREFIX.endswith("/"):
    S3_PREFIX += "/"

AWS_REGION  = os.environ.get("AWS_REGION", "us-east-1")
REQUEST_DELAY = 0.25  # seconds between requests to respect rate limits

# All 50 US state abbreviations + DC
ALL_STATES = [
    "AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "FL", "GA",
    "HI", "ID", "IL", "IN", "IA", "KS", "KY", "LA", "ME", "MD",
    "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH", "NJ",
    "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI", "SC",
    "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV", "WI", "WY",
    "DC",
]



# ---------------------------------------------------------------------------
# HTTP helper
# ---------------------------------------------------------------------------
_SESSION = requests.Session()
_SESSION.headers.update({"Accept": "application/json"})


def api_get(path: str, params: dict | None = None, timeout: int = 30) -> Any:
    if not API_KEY:
        log.error("DATA_GOV_API_KEY is not set. Add it to .env or export it.")
        sys.exit(1)

    query = {"API_KEY": API_KEY, **(params or {})}
    url = BASE_URL + path

    try:
        resp = _SESSION.get(url, params=query, timeout=timeout)
    except requests.exceptions.RequestException as exc:
        log.warning("Request failed for %s: %s", path, exc)
        return None

    if resp.status_code == 200:
        return resp.json()

    log.warning("HTTP %d for %s — %s", resp.status_code, path, resp.text[:200])
    return None


# ---------------------------------------------------------------------------
# Fetch + flatten
# ---------------------------------------------------------------------------

def fetch_agencies_for_state(state: str, extracted_at: str) -> list[dict]:
    """Fetch agencies for one state and return enriched record dicts.

    The API returns {county_name: [agency, ...]} — we flatten all counties
    and add ETL metadata fields to each record.
    """
    data = api_get(f"/agency/byStateAbbr/{state}")
    if not data:
        return []

    if not isinstance(data, dict):
        log.warning("Unexpected response shape for state %s: %s", state, type(data))
        return []

    rows = []
    for county_name, agencies in data.items():
        if not isinstance(agencies, list):
            continue
        for agency in agencies:
            if not isinstance(agency, dict):
                continue
            record = dict(agency)          # preserve all original API fields
            record["county_name"] = county_name
            record["state_abbr"]  = state  # ensure present even if API omits
            record["extracted_at"] = extracted_at
            rows.append(record)

    return rows


# ---------------------------------------------------------------------------
# S3 upload
# ---------------------------------------------------------------------------

def upload_to_s3(s3_client, rows: list[dict], state: str, timestamp: str) -> str:
    s3_key = f"{S3_PREFIX}agencies/state={state}/{state}_{timestamp}.ndjson"

    ndjson_body = "\n".join(json.dumps(row, default=str) for row in rows)

    s3_client.put_object(
        Bucket=S3_BUCKET,
        Key=s3_key,
        Body=ndjson_body.encode("utf-8"),
        ContentType="application/x-ndjson",
    )
    return s3_key


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    log.info("FBI CDE Agency ETL — bucket: s3://%s/%s", S3_BUCKET, S3_PREFIX)
    log.info("API key: %s...%s", API_KEY[:4], API_KEY[-4:])

    s3_client = boto3.client("s3", region_name=AWS_REGION)
    extracted_at = datetime.now(timezone.utc).isoformat()
    timestamp    = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")

    total_rows   = 0
    total_states = 0
    failed       = []

    for state in ALL_STATES:
        log.info("Fetching agencies for %s …", state)
        rows = fetch_agencies_for_state(state, extracted_at)

        if not rows:
            log.warning("  No data for %s — skipping upload.", state)
            failed.append(state)
            time.sleep(REQUEST_DELAY)
            continue

        s3_key = upload_to_s3(s3_client, rows, state, timestamp)
        log.info("  Uploaded %d agencies → s3://%s/%s", len(rows), S3_BUCKET, s3_key)
        total_rows   += len(rows)
        total_states += 1
        time.sleep(REQUEST_DELAY)

    log.info("Done. %d states uploaded, %d total agency rows.", total_states, total_rows)
    if failed:
        log.warning("States with no data: %s", ", ".join(failed))


if __name__ == "__main__":
    main()
