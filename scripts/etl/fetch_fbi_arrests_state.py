#!/usr/bin/env python3
"""
FBI CDE State Arrests ETL — fetch monthly arrest counts by state and offense code.

For each combination of state abbreviation × offense code, calls:
    GET https://api.usa.gov/crime/fbi/cde/arrest/state/{state}/{offense_code}
            ?type=counts&from=01-1999&to=03-2026&API_KEY=…

All rows are stacked (state + offense_code + observation_date + value) and
written to a single NDJSON file uploaded to:
    s3://{S3_BUCKET}/{S3_FBI_PREFIX}arrests/state/{timestamp}.ndjson

Environment variables (read from .env or shell):
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
# Config
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

BASE_URL      = "https://api.usa.gov/crime/fbi/cde"
API_KEY       = os.environ.get("DATA_GOV_API_KEY", "")
S3_BUCKET     = os.environ.get("S3_BUCKET", "fin-trade-extract-nosubs-bucket")
S3_PREFIX     = os.environ.get("S3_FBI_PREFIX", "fbi/")
if not S3_PREFIX.endswith("/"):
    S3_PREFIX += "/"

AWS_REGION    = os.environ.get("AWS_REGION", "us-east-1")
FROM_DATE     = "01-1999"
TO_DATE       = "03-2026"
REQUEST_DELAY = 0.25  # seconds between requests

STATE_CODES = [
    "AK", "AL", "AR", "AZ", "CA", "CO", "CT", "DC", "DE", "FL",
    "GA", "HI", "IA", "ID", "IL", "IN", "KS", "KY", "LA", "MA",
    "MD", "ME", "MI", "MN", "MO", "MS", "MT", "NC", "ND", "NE",
    "NH", "NJ", "NM", "NV", "NY", "OH", "OK", "OR", "PA", "RI",
    "SC", "SD", "TN", "TX", "UT", "VA", "VT", "WA", "WI", "WV",
    "WY",
]

OFFENSE_CODES = [
    310, 110, 50, 60, 101, 330, 290, 260, 150, 158, 157, 160, 159, 156, 153,
    152, 155, 154, 151, 280, 200, 180, 190, 173, 171, 172, 170, 102, 70, 270,
    12, 90, 11, 250, 140, 142, 141, 143, 23, 20, 30, 240, 55, 210, 300, 220,
    230,
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

def fetch_arrests_for_state_offense(state: str, offense_code: int, extracted_at: str) -> list[dict]:
    """Fetch monthly arrest counts for one state + offense code combination.

    The API returns:
      {
        "actuals": {"<STATE>": {"MM-YYYY": count, ...}},
        "rates":   {"<STATE>": {"MM-YYYY": rate,  ...}},
        ...
      }

    Each MM-YYYY entry is emitted as a separate monthly row.
    """
    path = f"/arrest/state/{state}/{offense_code}"
    params = {"type": "counts", "from": FROM_DATE, "to": TO_DATE}
    payload = api_get(path, params)

    if payload is None:
        return []

    if not isinstance(payload, dict):
        log.warning("Unexpected payload shape for %s/%d: %s", state, offense_code, type(payload))
        return []

    actuals = payload.get("actuals", {})
    if not isinstance(actuals, dict):
        log.warning("Unexpected 'actuals' shape for %s/%d: %s", state, offense_code, type(actuals))
        return []

    # Grab the first series dict (keyed by state abbreviation)
    series: dict | None = None
    for v in actuals.values():
        if isinstance(v, dict):
            series = v
            break

    if not series:
        log.warning("No actuals series found for %s/%d", state, offense_code)
        return []

    rows = []
    for date_str, count in sorted(series.items(), key=lambda x: (x[0].split("-")[1], x[0].split("-")[0])):
        if count is None:
            continue
        try:
            month_str, year_str = date_str.split("-")  # "MM-YYYY"
            observation_date = f"{year_str}-{month_str}-01"
            rows.append({
                "state":            state,
                "observation_date": observation_date,
                "value":            float(count),
                "offense_code":     offense_code,
                "extracted_at":     extracted_at,
            })
        except (ValueError, IndexError, TypeError):
            log.warning("Could not parse date '%s' for %s/%d", date_str, state, offense_code)

    return rows


# ---------------------------------------------------------------------------
# S3 upload
# ---------------------------------------------------------------------------

def upload_to_s3(s3_client, rows: list[dict], timestamp: str) -> str:
    s3_key = f"{S3_PREFIX}arrests/state/{timestamp}.ndjson"

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
    log.info("FBI State Arrests ETL — bucket: s3://%s/%s", S3_BUCKET, S3_PREFIX)
    if API_KEY:
        log.info("API key: %s...%s", API_KEY[:4], API_KEY[-4:])

    s3_client    = boto3.client("s3", region_name=AWS_REGION)
    extracted_at = datetime.now(timezone.utc).isoformat()
    timestamp    = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")

    unique_states  = list(dict.fromkeys(STATE_CODES))
    unique_codes   = list(dict.fromkeys(OFFENSE_CODES))
    total_combos   = len(unique_states) * len(unique_codes)

    log.info("Processing %d states × %d offense codes = %d API calls …",
             len(unique_states), len(unique_codes), total_combos)

    all_rows = []
    failed   = []
    combo    = 0

    for state in unique_states:
        for code in unique_codes:
            combo += 1
            log.info("  [%d/%d] %s / offense %d …", combo, total_combos, state, code)
            rows = fetch_arrests_for_state_offense(state, code, extracted_at)

            if not rows:
                log.warning("  No data for %s/%d — skipping.", state, code)
                failed.append((state, code))
            else:
                all_rows.extend(rows)

            time.sleep(REQUEST_DELAY)

    if not all_rows:
        log.error("No data collected — nothing to upload.")
        sys.exit(1)

    s3_key = upload_to_s3(s3_client, all_rows, timestamp)
    log.info(
        "Uploaded %d total rows across %d combos → s3://%s/%s",
        len(all_rows),
        total_combos - len(failed),
        S3_BUCKET,
        s3_key,
    )

    if failed:
        log.warning("Failed (state, offense_code) combos: %s", failed)

    log.info("Done.")


if __name__ == "__main__":
    main()
