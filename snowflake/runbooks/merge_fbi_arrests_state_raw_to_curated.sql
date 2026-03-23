-- =============================================================================
-- Merge FBI State Arrests RAW → CURATED
-- Deduplicates on (STATE, OFFENSE_CODE, OBSERVATION_DATE), keeping the most
-- recently extracted row, then upserts into CURATED_FBI.ARRESTS_STATE.
-- =============================================================================

USE DATABASE FIN_TRADE_EXTRACT_NOSUBS;
USE WAREHOUSE FIN_TRADE_WH;
USE ROLE ETL_ROLE;

CREATE SCHEMA IF NOT EXISTS CURATED_FBI;
USE SCHEMA CURATED_FBI;

-- ---------------------------------------------------------------------------
-- Target table
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS CURATED_FBI.ARRESTS_STATE (
    STATE            VARCHAR(5)    NOT NULL,
    OFFENSE_CODE     INTEGER       NOT NULL,
    OBSERVATION_DATE DATE          NOT NULL,
    VALUE            FLOAT,
    EXTRACTED_AT     TIMESTAMP_TZ,
    SOURCE_FILE      VARCHAR,
    LOADED_AT        TIMESTAMP_TZ  DEFAULT CURRENT_TIMESTAMP(),
    UPDATED_AT       TIMESTAMP_TZ  DEFAULT CURRENT_TIMESTAMP()
);

-- ---------------------------------------------------------------------------
-- Deduplicated view of the raw table (latest extract wins)
-- ---------------------------------------------------------------------------
WITH RANKED AS (
    SELECT
        STATE,
        OFFENSE_CODE,
        TRY_TO_DATE(OBSERVATION_DATE)    AS OBSERVATION_DATE,
        VALUE,
        TRY_TO_TIMESTAMP_TZ(EXTRACTED_AT) AS EXTRACTED_AT,
        SOURCE_FILE,
        ROW_NUMBER() OVER (
            PARTITION BY STATE, OFFENSE_CODE, OBSERVATION_DATE
            ORDER BY TRY_TO_TIMESTAMP_TZ(EXTRACTED_AT) DESC NULLS LAST,
                     LOADED_AT                         DESC NULLS LAST
        ) AS RN
    FROM RAW_FBI.FBI_ARRESTS_STATE_RAW
    WHERE OBSERVATION_DATE IS NOT NULL
)

MERGE INTO CURATED_FBI.ARRESTS_STATE AS TGT
USING (
    SELECT
        STATE,
        OFFENSE_CODE,
        OBSERVATION_DATE,
        VALUE,
        EXTRACTED_AT,
        SOURCE_FILE
    FROM RANKED
    WHERE RN = 1
) AS SRC
ON  TGT.STATE            = SRC.STATE
AND TGT.OFFENSE_CODE     = SRC.OFFENSE_CODE
AND TGT.OBSERVATION_DATE = SRC.OBSERVATION_DATE

WHEN MATCHED AND (
    TGT.VALUE            IS DISTINCT FROM SRC.VALUE
    OR TGT.EXTRACTED_AT  IS DISTINCT FROM SRC.EXTRACTED_AT
) THEN UPDATE SET
    TGT.VALUE        = SRC.VALUE,
    TGT.EXTRACTED_AT = SRC.EXTRACTED_AT,
    TGT.SOURCE_FILE  = SRC.SOURCE_FILE,
    TGT.UPDATED_AT   = CURRENT_TIMESTAMP()

WHEN NOT MATCHED THEN INSERT (
    STATE, OFFENSE_CODE, OBSERVATION_DATE,
    VALUE, EXTRACTED_AT, SOURCE_FILE,
    LOADED_AT, UPDATED_AT
) VALUES (
    SRC.STATE, SRC.OFFENSE_CODE, SRC.OBSERVATION_DATE,
    SRC.VALUE, SRC.EXTRACTED_AT, SRC.SOURCE_FILE,
    CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()
);

-- ---------------------------------------------------------------------------
-- Verify
-- ---------------------------------------------------------------------------
SELECT
    STATE,
    COUNT(DISTINCT OFFENSE_CODE) AS OFFENSE_CODES,
    MIN(OBSERVATION_DATE)        AS EARLIEST_MONTH,
    MAX(OBSERVATION_DATE)        AS LATEST_MONTH,
    COUNT(*)                     AS TOTAL_ROWS
FROM CURATED_FBI.ARRESTS_STATE
GROUP BY STATE
ORDER BY STATE;
