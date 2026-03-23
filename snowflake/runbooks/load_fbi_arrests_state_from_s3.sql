-- =============================================================================
-- Load FBI State Arrests from S3 (NDJSON) into Snowflake RAW_FBI schema
-- =============================================================================
-- Template variables (resolved by scripts/github_actions/snowflake_run_sql_file.py):
--   {{SQLSTR:S3_FBI_ARRESTS_STATE_URI}}   – s3://bucket/prefix/fbi/arrests/state/
--   {{SQLSTR:AWS_ACCESS_KEY_ID}}          – AWS access key
--   {{SQLSTR:AWS_SECRET_ACCESS_KEY}}      – AWS secret key
-- =============================================================================

USE DATABASE FIN_TRADE_EXTRACT_NOSUBS;
USE WAREHOUSE FIN_TRADE_WH;
USE ROLE ETL_ROLE;

CREATE SCHEMA IF NOT EXISTS RAW_FBI;
USE SCHEMA RAW_FBI;

-- ---------------------------------------------------------------------------
-- External stage pointing at the state arrests S3 folder
-- ---------------------------------------------------------------------------
CREATE OR REPLACE STAGE FBI_ARRESTS_STATE_STAGE
    URL = {{SQLSTR:S3_FBI_ARRESTS_STATE_URI}}
    CREDENTIALS = (
        AWS_KEY_ID     = {{SQLSTR:AWS_ACCESS_KEY_ID}}
        AWS_SECRET_KEY = {{SQLSTR:AWS_SECRET_ACCESS_KEY}}
    )
    FILE_FORMAT = (
        TYPE            = 'JSON'
        STRIP_OUTER_ARRAY = FALSE
    );

-- ---------------------------------------------------------------------------
-- Raw landing table (one row per NDJSON record)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS FBI_ARRESTS_STATE_RAW (
    STATE            STRING,
    OBSERVATION_DATE STRING,
    VALUE            FLOAT,
    OFFENSE_CODE     INTEGER,
    EXTRACTED_AT     STRING,
    SOURCE_FILE      STRING,
    LOADED_AT        TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP()
);

-- ---------------------------------------------------------------------------
-- Load — FORCE=TRUE re-loads files even if previously copied
-- ---------------------------------------------------------------------------
COPY INTO FBI_ARRESTS_STATE_RAW (
    STATE,
    OBSERVATION_DATE,
    VALUE,
    OFFENSE_CODE,
    EXTRACTED_AT,
    SOURCE_FILE
)
FROM (
    SELECT
        $1:state::STRING,
        $1:observation_date::STRING,
        $1:value::FLOAT,
        $1:offense_code::INTEGER,
        $1:extracted_at::STRING,
        METADATA$FILENAME
    FROM @FBI_ARRESTS_STATE_STAGE
)
FILE_FORMAT = (
    TYPE              = 'JSON'
    STRIP_OUTER_ARRAY = FALSE
)
FORCE = TRUE
ON_ERROR = 'CONTINUE';

-- ---------------------------------------------------------------------------
-- Verify: show date range and row counts by state
-- ---------------------------------------------------------------------------
SELECT
    STATE,
    COUNT(DISTINCT OFFENSE_CODE) AS OFFENSE_CODES,
    MIN(TRY_TO_DATE(OBSERVATION_DATE)) AS EARLIEST_MONTH,
    MAX(TRY_TO_DATE(OBSERVATION_DATE)) AS LATEST_MONTH,
    COUNT(*)                           AS TOTAL_ROWS
FROM FBI_ARRESTS_STATE_RAW
GROUP BY STATE
ORDER BY STATE;
