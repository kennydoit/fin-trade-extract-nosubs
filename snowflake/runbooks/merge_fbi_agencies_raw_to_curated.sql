USE DATABASE {{RAW:SNOWFLAKE_DATABASE}};
USE WAREHOUSE {{RAW:SNOWFLAKE_WAREHOUSE}};

-- Ensure curated schema exists and ETL role has access
CREATE SCHEMA IF NOT EXISTS CURATED_FBI;
USE SCHEMA CURATED_FBI;

-- Create curated table with typed columns extracted from RAW_DATA VARIANT
CREATE TABLE IF NOT EXISTS FBI_AGENCIES (
    ORI               VARCHAR(20),
    AGENCY_NAME       VARCHAR,
    AGENCY_TYPE_NAME  VARCHAR,
    STATE_ABBR        VARCHAR(2),
    STATE_NAME        VARCHAR,
    COUNTY_NAME       VARCHAR,
    IS_NIBRS          BOOLEAN,
    NIBRS_START_DATE  DATE,
    LATITUDE          FLOAT,
    LONGITUDE         FLOAT,
    EXTRACTED_AT      TIMESTAMP_TZ,
    SOURCE_FILE       VARCHAR,
    LOADED_AT         TIMESTAMP_TZ,
    UPDATED_AT        TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP()
);

-- Merge latest raw data into curated table, deduplicating on ORI
MERGE INTO FBI_AGENCIES target
USING (
    SELECT
        RAW_DATA:ori::VARCHAR                                        AS ori,
        RAW_DATA:agency_name::VARCHAR                                AS agency_name,
        RAW_DATA:agency_type_name::VARCHAR                           AS agency_type_name,
        RAW_DATA:state_abbr::VARCHAR                                 AS state_abbr,
        RAW_DATA:state_name::VARCHAR                                 AS state_name,
        RAW_DATA:county_name::VARCHAR                                AS county_name,
        RAW_DATA:is_nibrs::BOOLEAN                                   AS is_nibrs,
        TRY_TO_DATE(RAW_DATA:nibrs_start_date::VARCHAR)              AS nibrs_start_date,
        TRY_TO_DOUBLE(RAW_DATA:latitude::VARCHAR)                    AS latitude,
        TRY_TO_DOUBLE(RAW_DATA:longitude::VARCHAR)                   AS longitude,
        TRY_TO_TIMESTAMP_TZ(RAW_DATA:extracted_at::VARCHAR)          AS extracted_at,
        SOURCE_FILE,
        LOADED_AT
    FROM RAW_FBI.FBI_AGENCIES_RAW
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY RAW_DATA:ori::VARCHAR
        ORDER BY TRY_TO_TIMESTAMP_TZ(RAW_DATA:extracted_at::VARCHAR) DESC NULLS LAST,
                 LOADED_AT DESC NULLS LAST
    ) = 1
) source
ON target.ORI = source.ORI
WHEN MATCHED THEN UPDATE SET
    AGENCY_NAME      = source.AGENCY_NAME,
    AGENCY_TYPE_NAME = source.AGENCY_TYPE_NAME,
    STATE_ABBR       = source.STATE_ABBR,
    STATE_NAME       = source.STATE_NAME,
    COUNTY_NAME      = source.COUNTY_NAME,
    IS_NIBRS         = source.IS_NIBRS,
    NIBRS_START_DATE = source.NIBRS_START_DATE,
    LATITUDE         = source.LATITUDE,
    LONGITUDE        = source.LONGITUDE,
    EXTRACTED_AT     = source.EXTRACTED_AT,
    SOURCE_FILE      = source.SOURCE_FILE,
    LOADED_AT        = source.LOADED_AT,
    UPDATED_AT       = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN INSERT (
    ORI,
    AGENCY_NAME,
    AGENCY_TYPE_NAME,
    STATE_ABBR,
    STATE_NAME,
    COUNTY_NAME,
    IS_NIBRS,
    NIBRS_START_DATE,
    LATITUDE,
    LONGITUDE,
    EXTRACTED_AT,
    SOURCE_FILE,
    LOADED_AT,
    UPDATED_AT
) VALUES (
    source.ORI,
    source.AGENCY_NAME,
    source.AGENCY_TYPE_NAME,
    source.STATE_ABBR,
    source.STATE_NAME,
    source.COUNTY_NAME,
    source.IS_NIBRS,
    source.NIBRS_START_DATE,
    source.LATITUDE,
    source.LONGITUDE,
    source.EXTRACTED_AT,
    source.SOURCE_FILE,
    source.LOADED_AT,
    CURRENT_TIMESTAMP()
);

-- Verify
SELECT
    STATE_ABBR,
    COUNT(*) AS agency_count
FROM FBI_AGENCIES
GROUP BY STATE_ABBR
ORDER BY STATE_ABBR;
