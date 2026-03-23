USE DATABASE {{RAW:SNOWFLAKE_DATABASE}};
USE WAREHOUSE {{RAW:SNOWFLAKE_WAREHOUSE}};

-- Step 1: Create schema if it doesn't exist
CREATE SCHEMA IF NOT EXISTS RAW_FBI;
USE SCHEMA RAW_FBI;

-- Step 2: Create external stage pointing to the FBI national arrests S3 folder
CREATE OR REPLACE STAGE FBI_ARRESTS_NATIONAL_STAGE
    URL = {{SQLSTR:S3_FBI_ARRESTS_NATIONAL_URI}}
    CREDENTIALS = (
        AWS_KEY_ID     = {{SQLSTR:AWS_ACCESS_KEY_ID}}
        AWS_SECRET_KEY = {{SQLSTR:AWS_SECRET_ACCESS_KEY}}
        AWS_TOKEN      = {{OPT:AWS_SESSION_TOKEN}}
    )
    FILE_FORMAT = (
        TYPE = JSON
        STRIP_OUTER_ARRAY = FALSE   -- files are NDJSON (one JSON object per line)
    );

-- Step 3: Create raw table (VARIANT column preserves full API payload)
CREATE TABLE IF NOT EXISTS FBI_ARRESTS_NATIONAL_RAW (
    RAW_DATA    VARIANT,
    SOURCE_FILE VARCHAR,
    LOADED_AT   TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP()
);

-- Step 4: Copy all NDJSON files from S3 into the table
COPY INTO FBI_ARRESTS_NATIONAL_RAW (RAW_DATA, SOURCE_FILE, LOADED_AT)
FROM (
    SELECT
        $1::VARIANT,
        METADATA$FILENAME::VARCHAR,
        CURRENT_TIMESTAMP()
    FROM @FBI_ARRESTS_NATIONAL_STAGE (PATTERN => '.*[.]ndjson')
)
FILE_FORMAT = (
    TYPE = JSON
    STRIP_OUTER_ARRAY = FALSE
)
ON_ERROR = 'CONTINUE'
FORCE = TRUE;

-- Step 5: Verify load — count rows per offense code
SELECT
    RAW_DATA:offense_code::INTEGER  AS offense_code,
    COUNT(*)                        AS row_count,
    MIN(RAW_DATA:observation_date::DATE) AS earliest_month,
    MAX(RAW_DATA:observation_date::DATE) AS latest_month
FROM FBI_ARRESTS_NATIONAL_RAW
GROUP BY offense_code
ORDER BY offense_code;
