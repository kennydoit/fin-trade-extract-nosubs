USE DATABASE {{RAW:SNOWFLAKE_DATABASE}};
USE WAREHOUSE {{RAW:SNOWFLAKE_WAREHOUSE}};

-- Ensure curated schema exists
CREATE SCHEMA IF NOT EXISTS CURATED_FBI;
USE SCHEMA CURATED_FBI;

-- Create curated table with typed columns
CREATE TABLE IF NOT EXISTS ARRESTS_NATIONAL (
    OFFENSE_CODE      INTEGER      NOT NULL,
    OBSERVATION_DATE  DATE         NOT NULL,
    VALUE             FLOAT,
    RATE              FLOAT,
    POPULATION        BIGINT,
    EXTRACTED_AT      TIMESTAMP_TZ,
    SOURCE_FILE       VARCHAR,
    LOADED_AT         TIMESTAMP_TZ,
    UPDATED_AT        TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (OFFENSE_CODE, OBSERVATION_DATE)
);

-- Merge latest raw data into curated table, deduplicating on (OFFENSE_CODE, OBSERVATION_DATE)
MERGE INTO ARRESTS_NATIONAL target
USING (
    SELECT
        RAW_DATA:offense_code::INTEGER                                AS offense_code,
        TRY_TO_DATE(RAW_DATA:observation_date::VARCHAR)               AS observation_date,
        TRY_TO_DOUBLE(RAW_DATA:value::VARCHAR)                        AS value,
        TRY_TO_DOUBLE(RAW_DATA:rate::VARCHAR)                         AS rate,
        RAW_DATA:population::BIGINT                                   AS population,
        TRY_TO_TIMESTAMP_TZ(RAW_DATA:extracted_at::VARCHAR)           AS extracted_at,
        SOURCE_FILE,
        LOADED_AT
    FROM RAW_FBI.FBI_ARRESTS_NATIONAL_RAW
    WHERE RAW_DATA:offense_code IS NOT NULL
      AND RAW_DATA:observation_date IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY RAW_DATA:offense_code::INTEGER, TRY_TO_DATE(RAW_DATA:observation_date::VARCHAR)
        ORDER BY TRY_TO_TIMESTAMP_TZ(RAW_DATA:extracted_at::VARCHAR) DESC NULLS LAST,
                 LOADED_AT DESC NULLS LAST
    ) = 1
) source
ON  target.OFFENSE_CODE     = source.OFFENSE_CODE
AND target.OBSERVATION_DATE = source.OBSERVATION_DATE
WHEN MATCHED THEN UPDATE SET
    VALUE            = source.VALUE,
    RATE             = source.RATE,
    POPULATION       = source.POPULATION,
    EXTRACTED_AT     = source.EXTRACTED_AT,
    SOURCE_FILE      = source.SOURCE_FILE,
    LOADED_AT        = source.LOADED_AT,
    UPDATED_AT       = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN INSERT (
    OFFENSE_CODE,
    OBSERVATION_DATE,
    VALUE,
    RATE,
    POPULATION,
    EXTRACTED_AT,
    SOURCE_FILE,
    LOADED_AT,
    UPDATED_AT
) VALUES (
    source.OFFENSE_CODE,
    source.OBSERVATION_DATE,
    source.VALUE,
    source.RATE,
    source.POPULATION,
    source.EXTRACTED_AT,
    source.SOURCE_FILE,
    source.LOADED_AT,
    CURRENT_TIMESTAMP()
);

-- Verify — row counts per offense code in the curated table
SELECT
    OFFENSE_CODE,
    COUNT(*)               AS month_count,
    MIN(OBSERVATION_DATE)  AS earliest_month,
    MAX(OBSERVATION_DATE)  AS latest_month
FROM CURATED_FBI.ARRESTS_NATIONAL
GROUP BY OFFENSE_CODE
ORDER BY OFFENSE_CODE;
