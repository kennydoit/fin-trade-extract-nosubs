#!/usr/bin/env python3
"""
FBI Crime Data Explorer (CDE) EDA Script
=========================================
Queries the FBI CDE REST API (via api.usa.gov/crime/fbi/cde) and exports
an Excel workbook to eda/data/fbi_crime_eda.xlsx.

Authentication
--------------
The FBI CDE API uses a data.gov API key passed as the ``api_key`` query
parameter.  If you registered at https://api.data.gov/signup/ and set
``DATA_GOV_API_KEY`` in your .env, this script picks it up automatically.

  NOTE: If requests return HTTP 403 "Missing Authentication Token" you may
  need to register your key specifically with the FBI CDE API Gateway.
  Visit https://cde.ucr.cjis.gov/ to verify access.

Endpoints covered
-----------------
1.  National participation    – GET /participation/national
2.  State participation       – GET /participation/states
3.  National offenses by year – GET /offense/national/{variable}
4.  State offenses by year    – GET /offense/state/{stateAbbr}/{variable}
5.  Arrests by state          – GET /arrest/states/offense/{state}/all/{y1}/{y2}
6.  Hate crimes by state      – GET /hate-crime/state/{stateAbbr}
7.  Agency counts by state    – GET /agency  (paged)

Usage
-----
    python scripts/eda/fbi_data_explore.py            # run all demos
    python scripts/eda/fbi_data_explore.py offenses   # single section name

The section names match the function names below:
  participation_national | participation_states | offenses_national |
  offenses_by_state      | arrests_by_state     | hate_crimes       | agencies
"""

import logging
import os
import sys
import time
from pathlib import Path
from typing import Any

import pandas as pd
import requests

# ---------------------------------------------------------------------------
# Bootstrap — load .env from workspace root
# ---------------------------------------------------------------------------
_WORKSPACE_ROOT = Path(__file__).resolve().parents[2]

def _load_dotenv(env_path: Path) -> None:
    """Minimal .env parser — no third-party dependency required."""
    if not env_path.exists():
        return
    for raw_line in env_path.read_text().splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
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

BASE_URL  = "https://api.usa.gov/crime/fbi/cde"
API_KEY   = os.environ.get("DATA_GOV_API_KEY", "")
FROM_YEAR = 2010
TO_YEAR   = 2022

OUTPUT_PATH = _WORKSPACE_ROOT / "eda" / "data" / "fbi_crime_eda.xlsx"

# A representative set of state abbreviations used for multi-state queries
SAMPLE_STATES = ["AL", "CA", "FL", "IL", "NY", "OH", "TX", "WA"]

# FBI CDE offense variable slugs
OFFENSE_VARS = [
    "violent-crime",
    "property-crime",
    "homicide",
    "rape",
    "robbery",
    "assault",
    "burglary",
    "larceny",
    "motor-vehicle-theft",
    "arson",
]

DIVIDER = "\n" + "=" * 70 + "\n"

# ---------------------------------------------------------------------------
# HTTP helper
# ---------------------------------------------------------------------------

_SESSION = requests.Session()
_SESSION.headers.update({"Accept": "application/json"})


def api_get(path: str, params: dict | None = None, timeout: int = 30) -> Any:
    """
    GET ``BASE_URL + path`` with the API key injected.

    Returns parsed JSON on 2xx, logs a warning and returns None otherwise.
    Respects a short inter-request delay to stay within rate limits.
    """
    if not API_KEY:
        log.error(
            "DATA_GOV_API_KEY is not set.  Export the variable or add it to .env."
        )
        return None

    query = {"API_KEY": API_KEY, **(params or {})}
    url = BASE_URL + path

    try:
        resp = _SESSION.get(url, params=query, timeout=timeout)
    except requests.exceptions.RequestException as exc:
        log.warning("Request failed for %s: %s", path, exc)
        return None

    if resp.status_code == 200:
        time.sleep(0.25)  # ~4 req/sec — well inside rate limits
        try:
            return resp.json()
        except ValueError:
            log.warning("Non-JSON 200 response for %s", path)
            return None

    log.warning(
        "HTTP %s for %s — %s",
        resp.status_code,
        path,
        resp.text[:200],
    )
    return None


# ---------------------------------------------------------------------------
# Display helpers
# ---------------------------------------------------------------------------

def show(label: str, obj: Any) -> None:
    """Print a labelled object (DataFrame, dict, or scalar) with a header."""
    print(f"\n--- {label} ---")
    if isinstance(obj, pd.DataFrame):
        print(obj.to_string() if len(obj) <= 20 else obj.head(15).to_string())
    elif isinstance(obj, dict):
        for k, v in list(obj.items())[:30]:
            print(f"  {k}: {v}")
    elif isinstance(obj, list):
        for item in obj[:15]:
            print(f"  {item}")
    else:
        print(obj)
    print()


def _to_df(data: Any, label: str = "") -> pd.DataFrame | None:
    """Coerce API response to a DataFrame; log and return None on failure."""
    if data is None:
        return None
    if isinstance(data, list):
        df = pd.DataFrame(data)
    elif isinstance(data, dict):
        # Some endpoints wrap the list under a key; find the first list value
        for v in data.values():
            if isinstance(v, list) and v:
                df = pd.DataFrame(v)
                break
        else:
            df = pd.DataFrame([data])
    else:
        log.warning("Unexpected data type (%s) for %s", type(data), label)
        return None
    return df if not df.empty else None


# ===========================================================================
# Section 1 — National Participation
# ===========================================================================

def participation_national() -> pd.DataFrame | None:
    """
    GET /participation/national

    Returns the number of agencies and population covered each year across
    the entire United States.  Good for understanding data completeness.
    """
    print(DIVIDER + "1. NATIONAL PARTICIPATION")

    data = api_get("/participation/national")
    df = _to_df(data, "participation/national")
    if df is None:
        print("  No data returned.")
        return None

    if "data" in df.columns:
        # Some responses nest the payload in a 'data' key
        df = pd.json_normalize(df["data"])

    df = df.sort_values("year") if "year" in df.columns else df
    show("National Participation (by year)", df)
    return df


# ===========================================================================
# Section 2 — State Participation
# ===========================================================================

def participation_states() -> pd.DataFrame | None:
    """
    GET /participation/states

    Participation rates, agency counts, and covered population for every
    state.  Useful for weighting cross-state crime comparisons.
    """
    print(DIVIDER + "2. STATE PARTICIPATION")

    data = api_get("/participation/states")
    df = _to_df(data, "participation/states")
    if df is None:
        print("  No data returned.")
        return None

    # Flatten nested 'data' list when present
    if "data" in df.columns and isinstance(df["data"].iloc[0], list):
        records = []
        for _, row in df.iterrows():
            for entry in row["data"]:
                entry_copy = dict(entry)
                entry_copy.setdefault("state_abbr", row.get("state_abbr"))
                records.append(entry_copy)
        df = pd.DataFrame(records)

    df = df.sort_values(
        ["state_abbr", "year"] if "year" in df.columns else ["state_abbr"]
    ) if "state_abbr" in df.columns else df

    show("State Participation (first 20 rows)", df)
    return df


# ===========================================================================
# Section 3 — National Offense Trends by Year
# ===========================================================================

def offenses_national() -> dict[str, pd.DataFrame]:
    """
    GET /offense/national/{variable}?from=…&to=…

    Pulls every major offense category at the national level and prints
    a combined per-year summary.
    """
    print(DIVIDER + "3. NATIONAL OFFENSE TRENDS")

    frames: dict[str, pd.DataFrame] = {}
    for var in OFFENSE_VARS:
        path = f"/offense/national/{var}"
        data = api_get(path, {"from": FROM_YEAR, "to": TO_YEAR})
        df = _to_df(data, path)
        if df is None:
            log.info("  Skipping %s — no data.", var)
            continue
        df["offense_type"] = var
        frames[var] = df
        log.info("  Fetched national %s: %d rows", var, len(df))

    if not frames:
        print("  No national offense data returned.")
        return frames

    combined = pd.concat(frames.values(), ignore_index=True)
    if "year" in combined.columns:
        combined = combined.sort_values(["year", "offense_type"])

    show("National Offenses (sample)", combined)
    return frames


# ===========================================================================
# Section 4 — State Offenses by Year  (the primary interest)
# ===========================================================================

def offenses_by_state() -> pd.DataFrame | None:
    """
    GET /offense/state/{stateAbbr}/{variable}?from=…&to=…

    For each sample state, retrieves 'all' offense categories and organises
    results into a wide-format DataFrame indexed by (state, year).

    Produces two views:
      - Raw long-format table (every row = one state × year × offense)
      - Pivot: rows = year, columns = state, values = violent-crime count
    """
    print(DIVIDER + "4. STATE OFFENSES BY YEAR")

    records = []
    for state in SAMPLE_STATES:
        path = f"/offense/state/{state}/all"
        data = api_get(path, {"from": FROM_YEAR, "to": TO_YEAR})
        df = _to_df(data, path)
        if df is None:
            log.info("  Skipping state %s — no data.", state)
            continue
        df["state_abbr"] = state
        records.append(df)
        log.info("  Fetched offenses for %s: %d rows", state, len(df))

    if not records:
        print("  No state offense data returned.")
        return None

    long_df = pd.concat(records, ignore_index=True)
    if "year" in long_df.columns:
        long_df["year"] = long_df["year"].astype(int)
        long_df = long_df.sort_values(["state_abbr", "year"])

    show("State Offenses — long format (sample)", long_df)

    # Pivot: violent-crime count by year × state (if the column exists)
    count_col = next(
        (c for c in long_df.columns if "count" in c.lower() or "actual" in c.lower()),
        None,
    )
    offense_col = next(
        (c for c in long_df.columns if "offense" in c.lower() or "crime_type" in c.lower()),
        None,
    )

    if count_col and offense_col and "year" in long_df.columns:
        vc = long_df[long_df[offense_col].str.contains("violent", case=False, na=False)]
        if not vc.empty:
            pivot = vc.pivot_table(
                index="year",
                columns="state_abbr",
                values=count_col,
                aggfunc="sum",
            )
            show("Violent Crime by Year × State (pivot)", pivot)

    return long_df


# ===========================================================================
# Section 5 — Arrests by State
# ===========================================================================

def arrests_by_state() -> pd.DataFrame | None:
    """
    GET /arrest/states/offense/{stateAbbr}/all/{startYear}/{endYear}

    Arrest totals (all offense types) for the sample states.
    """
    print(DIVIDER + "5. ARRESTS BY STATE")

    records = []
    for state in SAMPLE_STATES[:4]:  # limit to 4 states to keep runtime short
        path = f"/arrest/states/offense/{state}/all/{FROM_YEAR}/{TO_YEAR}"
        data = api_get(path)
        df = _to_df(data, path)
        if df is None:
            log.info("  Skipping arrests %s — no data.", state)
            continue
        df["state_abbr"] = state
        records.append(df)
        log.info("  Fetched arrests for %s: %d rows", state, len(df))

    if not records:
        print("  No arrest data returned.")
        return None

    combined = pd.concat(records, ignore_index=True)
    if "year" in combined.columns:
        combined = combined.sort_values(["state_abbr", "year"])
    show("Arrests by State", combined)
    return combined


# ===========================================================================
# Section 6 — Hate Crimes by State
# ===========================================================================

def hate_crimes() -> pd.DataFrame | None:
    """
    GET /hate-crime/state/{stateAbbr}

    Hate crime incident counts per year for the sample states.
    """
    print(DIVIDER + "6. HATE CRIMES BY STATE")

    records = []
    for state in SAMPLE_STATES:
        path = f"/hate-crime/state/{state}"
        data = api_get(path, {"from": FROM_YEAR, "to": TO_YEAR})
        df = _to_df(data, path)
        if df is None:
            log.info("  Skipping hate crime %s — no data.", state)
            continue
        df["state_abbr"] = state
        records.append(df)
        log.info("  Fetched hate crimes for %s: %d rows", state, len(df))

    if not records:
        print("  No hate crime data returned.")
        return None

    combined = pd.concat(records, ignore_index=True)
    if "year" in combined.columns:
        combined = combined.sort_values(["state_abbr", "year"])
    show("Hate Crimes by State", combined)
    return combined


# ===========================================================================
# Section 7 — Agency Counts by State
# ===========================================================================

def agencies() -> pd.DataFrame | None:
    """
    GET /agency?state_abbr={state}&page_size=100

    Lists agencies (e.g., city police, sheriff offices) and their
    reporting status.  Useful for understanding data completeness by
    jurisdiction.
    """
    print(DIVIDER + "7. AGENCIES BY STATE")

    # Fetch agencies for a single state as a representative sample
    state = "CA"
    path = "/agency"
    data = api_get(path, {"state_abbr": state, "page_size": 100})
    df = _to_df(data, path)
    if df is None:
        print("  No agency data returned.")
        return None

    show(f"Agencies in {state} (first 20 rows)", df)
    return df


# ===========================================================================
# Excel export
# ===========================================================================

def write_excel(sheets: dict[str, pd.DataFrame | None]) -> None:
    """Write non-empty DataFrames to separate sheets in one .xlsx file."""
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    valid_sheets = {k: v for k, v in sheets.items() if v is not None and not v.empty}

    if not valid_sheets:
        log.warning("No data to export — skipping Excel write.")
        return

    with pd.ExcelWriter(OUTPUT_PATH, engine="openpyxl") as writer:
        for sheet_name, df in valid_sheets.items():
            safe_name = sheet_name[:31]  # Excel sheet name limit
            df.to_excel(writer, sheet_name=safe_name, index=False)
            log.info("  Sheet '%s': %d rows × %d cols", safe_name, *df.shape)

    log.info("Workbook saved → %s", OUTPUT_PATH)


# ===========================================================================
# Entry point
# ===========================================================================

_DEMOS: dict[str, callable] = {
    "participation_national": participation_national,
    "participation_states":   participation_states,
    "offenses_national":      offenses_national,
    "offenses_by_state":      offenses_by_state,
    "arrests_by_state":       arrests_by_state,
    "hate_crimes":            hate_crimes,
    "agencies":               agencies,
}


def main() -> None:
    if not API_KEY:
        log.error(
            "DATA_GOV_API_KEY is not set.  "
            "Add it to .env or export it in your shell."
        )
        sys.exit(1)

    log.info("FBI CDE EDA  |  years %s–%s  |  base: %s", FROM_YEAR, TO_YEAR, BASE_URL)
    log.info("Key: %s...%s", API_KEY[:4], API_KEY[-4:])  # partial, never log full key

    # Optionally run a single section
    run_only: set[str] = set()
    for arg in sys.argv[1:]:
        if arg in _DEMOS:
            run_only.add(arg)
        else:
            log.warning("Unknown section '%s'. Valid: %s", arg, ", ".join(_DEMOS))

    excel_sheets: dict[str, pd.DataFrame | None] = {}
    for name, fn in _DEMOS.items():
        if run_only and name not in run_only:
            continue
        result = fn()
        # offenses_national returns a dict — flatten frames for Excel
        if isinstance(result, dict):
            for var, df in result.items():
                excel_sheets[f"offense_nat_{var[:20]}"] = df
        else:
            excel_sheets[name] = result

    write_excel(excel_sheets)


if __name__ == "__main__":
    main()
