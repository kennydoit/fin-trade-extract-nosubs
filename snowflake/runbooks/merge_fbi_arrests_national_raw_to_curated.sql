USE DATABASE {{RAW:SNOWFLAKE_DATABASE}};
USE WAREHOUSE {{RAW:SNOWFLAKE_WAREHOUSE}};

-- =============================================================================
-- Ensure curated schema and unified ARRESTS table exist
-- =============================================================================
CREATE SCHEMA IF NOT EXISTS CURATED_FBI;
USE SCHEMA CURATED_FBI;

-- Unified arrests table: holds NATIONAL, STATE, and ORI data in one place.
-- Primary key includes geography columns so all three granularities can coexist.
CREATE TABLE IF NOT EXISTS ARRESTS (
    GEOGRAPHY_LEVEL   VARCHAR(10)  NOT NULL,   -- 'NATIONAL' | 'STATE' | 'ORI'
    GEOGRAPHY_VALUE   VARCHAR(20)  NOT NULL,   -- 'National', 2-char state code, or ORI code
    OFFENSE_CODE      INTEGER      NOT NULL,
    OBSERVATION_DATE  DATE         NOT NULL,
    VALUE             FLOAT,
    RATE              FLOAT,
    POPULATION        BIGINT,
    EXTRACTED_AT      TIMESTAMP_TZ,
    SOURCE_FILE       VARCHAR,
    LOADED_AT         TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP(),
    UPDATED_AT        TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (GEOGRAPHY_LEVEL, GEOGRAPHY_VALUE, OFFENSE_CODE, OBSERVATION_DATE)
);

-- =============================================================================
-- Merge national raw data → CURATED_FBI.ARRESTS
-- Deduplicates on (OFFENSE_CODE, OBSERVATION_DATE), keeps latest extract.
-- Geography columns are pinned to NATIONAL / 'National'.
-- =============================================================================
MERGE INTO CURATED_FBI.ARRESTS AS TGT
USING (
    WITH RANKED AS (
        SELECT
            'NATIONAL'                                                    AS GEOGRAPHY_LEVEL,
            'National'                                                    AS GEOGRAPHY_VALUE,
            RAW_DATA:offense_code::INTEGER                                AS OFFENSE_CODE,
            TRY_TO_DATE(RAW_DATA:observation_date::VARCHAR)               AS OBSERVATION_DATE,
            TRY_TO_DOUBLE(RAW_DATA:value::VARCHAR)                        AS VALUE,
            TRY_TO_DOUBLE(RAW_DATA:rate::VARCHAR)                         AS RATE,
            RAW_DATA:population::BIGINT                                   AS POPULATION,
            TRY_TO_TIMESTAMP_TZ(RAW_DATA:extracted_at::VARCHAR)           AS EXTRACTED_AT,
            SOURCE_FILE,
            LOADED_AT,
            ROW_NUMBER() OVER (
                PARTITION BY RAW_DATA:offense_code::INTEGER,
                             TRY_TO_DATE(RAW_DATA:observation_date::VARCHAR)
                ORDER BY TRY_TO_TIMESTAMP_TZ(RAW_DATA:extracted_at::VARCHAR) DESC NULLS LAST,
                         LOADED_AT DESC NULLS LAST
            ) AS RN
        FROM RAW_FBI.FBI_ARRESTS_NATIONAL_RAW
        WHERE RAW_DATA:offense_code IS NOT NULL
          AND RAW_DATA:observation_date IS NOT NULL
    )
    SELECT
        GEOGRAPHY_LEVEL,
        GEOGRAPHY_VALUE,
        OFFENSE_CODE,
        OBSERVATION_DATE,
        VALUE,
        RATE,
        POPULATION,
        EXTRACTED_AT,
        SOURCE_FILE,
        LOADED_AT
    FROM RANKED
    WHERE RN = 1
) AS SRC
ON  TGT.GEOGRAPHY_LEVEL  = SRC.GEOGRAPHY_LEVEL
AND TGT.GEOGRAPHY_VALUE  = SRC.GEOGRAPHY_VALUE
AND TGT.OFFENSE_CODE     = SRC.OFFENSE_CODE
AND TGT.OBSERVATION_DATE = SRC.OBSERVATION_DATE
WHEN MATCHED AND (
    TGT.VALUE        IS DISTINCT FROM SRC.VALUE
    OR TGT.RATE      IS DISTINCT FROM SRC.RATE
    OR TGT.POPULATION IS DISTINCT FROM SRC.POPULATION
    OR TGT.EXTRACTED_AT IS DISTINCT FROM SRC.EXTRACTED_AT
) THEN UPDATE SET
    TGT.VALUE        = SRC.VALUE,
    TGT.RATE         = SRC.RATE,
    TGT.POPULATION   = SRC.POPULATION,
    TGT.EXTRACTED_AT = SRC.EXTRACTED_AT,
    TGT.SOURCE_FILE  = SRC.SOURCE_FILE,
    TGT.LOADED_AT    = SRC.LOADED_AT,
    TGT.UPDATED_AT   = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN INSERT (
    GEOGRAPHY_LEVEL,
    GEOGRAPHY_VALUE,
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
    SRC.GEOGRAPHY_LEVEL,
    SRC.GEOGRAPHY_VALUE,
    SRC.OFFENSE_CODE,
    SRC.OBSERVATION_DATE,
    SRC.VALUE,
    SRC.RATE,
    SRC.POPULATION,
    SRC.EXTRACTED_AT,
    SRC.SOURCE_FILE,
    SRC.LOADED_AT,
    CURRENT_TIMESTAMP()
);

-- =============================================================================
-- Verify — row counts for national data in the unified curated table
-- =============================================================================
SELECT
    GEOGRAPHY_LEVEL,
    OFFENSE_CODE,
    COUNT(*)               AS month_count,
    MIN(OBSERVATION_DATE)  AS earliest_month,
    MAX(OBSERVATION_DATE)  AS latest_month
FROM CURATED_FBI.ARRESTS
WHERE GEOGRAPHY_LEVEL = 'NATIONAL'
GROUP BY GEOGRAPHY_LEVEL, OFFENSE_CODE
ORDER BY OFFENSE_CODE;
