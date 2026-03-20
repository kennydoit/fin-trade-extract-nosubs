USE DATABASE {{RAW:SNOWFLAKE_DATABASE}};
USE WAREHOUSE {{RAW:SNOWFLAKE_WAREHOUSE}};

-- Ensure curated schema exists
CREATE SCHEMA IF NOT EXISTS CURATED_FBI;
USE SCHEMA CURATED_FBI;

-- Create curated table with typed columns
CREATE TABLE IF NOT EXISTS ARRESTS_NATIONAL (
    OFFENSE_CODE  INTEGER,
    DATA_YEAR     INTEGER,
    VALUE         FLOAT,
    EXTRACTED_AT  TIMESTAMP_TZ,
    SOURCE_FILE   VARCHAR,
    LOADED_AT     TIMESTAMP_TZ,
    UPDATED_AT    TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP()
);

-- Merge latest raw data into curated table, deduplicating on (OFFENSE_CODE, DATA_YEAR)
MERGE INTO ARRESTS_NATIONAL target
USING (
    SELECT
        RAW_DATA:offense_code::INTEGER                                AS offense_code,
        RAW_DATA:data_year::INTEGER                                   AS data_year,
        TRY_TO_DOUBLE(RAW_DATA:value::VARCHAR)                        AS value,
        TRY_TO_TIMESTAMP_TZ(RAW_DATA:extracted_at::VARCHAR)           AS extracted_at,
        SOURCE_FILE,
        LOADED_AT
    FROM RAW_FBI.FBI_ARRESTS_NATIONAL_RAW
    WHERE RAW_DATA:offense_code IS NOT NULL
      AND RAW_DATA:data_year   IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY RAW_DATA:offense_code::INTEGER, RAW_DATA:data_year::INTEGER
        ORDER BY TRY_TO_TIMESTAMP_TZ(RAW_DATA:extracted_at::VARCHAR) DESC NULLS LAST,
                 LOADED_AT DESC NULLS LAST
    ) = 1
) source
ON  target.OFFENSE_CODE = source.OFFENSE_CODE
AND target.DATA_YEAR    = source.DATA_YEAR
WHEN MATCHED THEN UPDATE SET
    VALUE        = source.VALUE,
    EXTRACTED_AT = source.EXTRACTED_AT,
    SOURCE_FILE  = source.SOURCE_FILE,
    LOADED_AT    = source.LOADED_AT,
    UPDATED_AT   = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN INSERT (
    OFFENSE_CODE,
    DATA_YEAR,
    VALUE,
    EXTRACTED_AT,
    SOURCE_FILE,
    LOADED_AT,
    UPDATED_AT
) VALUES (
    source.OFFENSE_CODE,
    source.DATA_YEAR,
    source.VALUE,
    source.EXTRACTED_AT,
    source.SOURCE_FILE,
    source.LOADED_AT,
    CURRENT_TIMESTAMP()
);

-- Verify — row counts per offense code in the curated table
SELECT
    OFFENSE_CODE,
    COUNT(*)     AS year_count,
    MIN(DATA_YEAR) AS earliest_year,
    MAX(DATA_YEAR) AS latest_year
FROM CURATED_FBI.ARRESTS_NATIONAL
GROUP BY OFFENSE_CODE
ORDER BY OFFENSE_CODE;
