USE DATABASE {{RAW:SNOWFLAKE_DATABASE}};
USE WAREHOUSE {{RAW:SNOWFLAKE_WAREHOUSE}};

-- Step 1: Create schema if it doesn't exist
CREATE SCHEMA IF NOT EXISTS RAW_FBI;
USE SCHEMA RAW_FBI;

-- Step 2: Create external stage pointing to the FBI agencies S3 folder
CREATE OR REPLACE STAGE FBI_AGENCIES_STAGE
    URL = {{SQLSTR:S3_FBI_AGENCIES_URI}}
    CREDENTIALS = (
        AWS_KEY_ID     = {{SQLSTR:AWS_ACCESS_KEY_ID}}
        AWS_SECRET_KEY = {{SQLSTR:AWS_SECRET_ACCESS_KEY}}
        AWS_TOKEN      = {{OPT:AWS_SESSION_TOKEN}}
    )
    FILE_FORMAT = (
        TYPE = JSON
        STRIP_OUTER_ARRAY = FALSE   -- files are NDJSON (one JSON object per line)
    );

-- Step 3: Create target table (raw VARIANT column preserves full API payload)
CREATE OR REPLACE TABLE FBI_AGENCIES_RAW (
    RAW_DATA    VARIANT,
    SOURCE_FILE VARCHAR,
    LOADED_AT   TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP()
);

-- Step 4: Copy all state NDJSON files from S3 into the table
COPY INTO FBI_AGENCIES_RAW (RAW_DATA, SOURCE_FILE, LOADED_AT)
FROM (
    SELECT
        $1::VARIANT,
        METADATA$FILENAME::VARCHAR,
        CURRENT_TIMESTAMP()
    FROM @FBI_AGENCIES_STAGE (PATTERN => '.*[.]ndjson')
)
FILE_FORMAT = (
    TYPE = JSON
    STRIP_OUTER_ARRAY = FALSE
)
ON_ERROR = 'CONTINUE'
FORCE = TRUE;

-- Step 5: Verify load — count agencies per state using VARIANT extraction
SELECT
    RAW_DATA:state_abbr::VARCHAR  AS state_abbr,
    COUNT(*)                      AS agency_count
FROM FBI_AGENCIES_RAW
GROUP BY state_abbr
ORDER BY state_abbr;
