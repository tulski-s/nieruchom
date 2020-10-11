
-- create main schema
CREATE SCHEMA dwh;


-- create staging table for otodom.pl
CREATE TABLE stg_otodom (
    ds DATE
    , source VARCHAR
    , offer_source_id VARCHAR
    , offer_type VARCHAR
    , offer_title VARCHAR
    , offer_url VARCHAR
    , offer_location_raw VARCHAR
    , province VARCHAR
    , county VARCHAR
    , city VARCHAR
    , district VARCHAR
    , neighbourhood VARCHAR
    , no_rooms INTEGER
    , price DECIMAL(9, 2) -- up to millions with 2 digits in fraction,
    , area REAL
    , offer_source VARCHAR
);

-- create working table for otodom load
CREATE TABLE wrk_otodom (
    sk_offer INTEGER
    , offer_source VARCHAR
    , offer_source_id VARCHAR
    , offer_type VARCHAR
    , offer_title VARCHAR
    , offer_url VARCHAR
    , offer_location_raw VARCHAR
    , province VARCHAR
    , county VARCHAR
    , city VARCHAR
    , district VARCHAR
    , neighbourhood VARCHAR
    , no_rooms INTEGER
    , price DECIMAL(9, 2) -- up to millions with 2 digits in fraction,
    , area REAL
    , etl_action VARCHAR
    , ds DATE
)


-- create ETL tracker table to maintain proper order of loading
CREATE TABLE etl_tracker (
    table_name VARCHAR
    , ds DATE
    , stg_loaded BOOLEAN
    , stg_load_ts TIMESTAMP WITHOUT TIME ZONE
    , dwh_loaded BOOLEAN
    , dwh_load_ts TIMESTAMP WITHOUT TIME ZONE
);


-- create main offers table
CREATE TABLE offers (
    sk_offer BIGSERIAL PRIMARY KEY
    , offer_source VARCHAR
    , offer_source_id VARCHAR
    , offer_type VARCHAR
    , offer_title VARCHAR
    , offer_url VARCHAR
    , offer_location_raw VARCHAR
    , province VARCHAR
    , county VARCHAR
    , city VARCHAR
    , district VARCHAR
    , neighbourhood VARCHAR
    , no_rooms INTEGER
    , price DECIMAL(9, 2) -- up to millions with 2 digits in fraction,
    , area REAL
    , offer_first_seen DATE
    , offer_last_seen DATE
    , offer_days_total INTEGER
    , row_actv_flg BOOLEAN
    , price_start  DATE
    , price_end DATE
);

  
