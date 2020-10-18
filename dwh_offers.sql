/*
Logic will be executed within single query_dwh function so that its contained within one transaction
*/

-- empty out working table (should be empty, but do it anyway as a safeguard)
TRUNCATE TABLE wrk_{scraper_id};

-- insert into working table. determine action based on internal id. dedup.
INSERT INTO wrk_{scraper_id}
SELECT
    sk_offer
    , offer_source
    , offer_source_id
    , offer_type
    , offer_title
    , offer_url
    , offer_location_raw
    , province
    , county
    , city
    , district
    , neighbourhood
    , no_rooms
    , price
    , area
    , etl_action
    , ds
FROM
    (
        SELECT
          b.sk_offer
          , stg.offer_source
          , stg.offer_source_id
          , stg.offer_type
          , stg.offer_title
          , stg.offer_url
          , stg.offer_location_raw
          , stg.province
          , stg.county
          , stg.city
          , stg.district
          , stg.neighbourhood
          , stg.no_rooms
          , stg.price
          , stg.area
          , CASE 
              WHEN b.sk_offer IS NULL THEN 'insert'
              WHEN (b.sk_offer IS NOT NULL AND stg.price = b.price) THEN 'update'
              WHEN (b.sk_offer IS NOT NULL AND stg.price <> b.price) THEN 'SCD2'
            END AS etl_action
          , stg.ds
          , ROW_NUMBER() OVER (PARTITION BY stg.offer_source_id) AS rn
        FROM
          stg_{scraper_id} stg
        LEFT JOIN
          offers b
        ON
          stg.offer_source_id = b.offer_source_id
        WHERE
          stg.ds = '{ds}'
          AND 
            CASE 
              WHEN b.row_actv_flg IS NOT NULL
              THEN b.row_actv_flg = True
              ELSE True
            END
    ) x
WHERE 
  -- deduplicates offers with same source offer id
  x.rn = 1
;

-- insert completly new offers
INSERT INTO offers (
    offer_source
    , offer_source_id
    , offer_type
    , offer_title
    , offer_url
    , offer_location_raw
    , province
    , county
    , city
    , district
    , neighbourhood
    , no_rooms
    , price
    , area
    , offer_first_seen
    , offer_last_seen
    , offer_days_total
    , row_actv_flg
    , price_start
    , price_end
)
SELECT
    offer_source
    , offer_source_id
    , offer_type
    , offer_title
    , offer_url
    , offer_location_raw
    , province
    , county
    , city
    , district
    , neighbourhood
    , no_rooms
    , price
    , area
    , ds AS offer_first_seen
    , ds AS offer_last_seen
    , 1 offer_days_total
    , True AS row_actv_flg
    , ds AS price_start
    , '9999-12-31'::DATE AS price_end
FROM
    wrk_{scraper_id}
WHERE
    etl_action = 'insert'
;

-- update offers without price change
UPDATE offers o
SET
    offer_last_seen = n.offer_last_seen
    , offer_days_total = n.offer_days_total
FROM
    (
        SELECT
            b.sk_offer
            , wrk.ds AS offer_last_seen
            , (wrk.ds - b.offer_first_seen) + 1 AS offer_days_total
        FROM
            offers b
        INNER JOIN
            wrk_{scraper_id} wrk
        ON
            b.sk_offer = wrk.sk_offer
        WHERE
            wrk.etl_action = 'update'
    ) n
WHERE
    o.sk_offer = n.sk_offer
;

-- "close" offers with price change
UPDATE offers o
SET
    row_actv_flg = n.row_actv_flg
    , price_end = n.price_end
FROM
    (
        SELECT
            b.sk_offer
            , False AS row_actv_flg
            , b.offer_last_seen AS price_end
        FROM
            offers b
        INNER JOIN
            wrk_{scraper_id} wrk
        ON
            b.sk_offer = wrk.sk_offer
        WHERE
            wrk.etl_action = 'SCD2'
    ) n
WHERE
    o.sk_offer = n.sk_offer
;

-- create new versions for offers with price change
INSERT INTO offers (
    offer_source
    , offer_source_id
    , offer_type
    , offer_title
    , offer_url
    , offer_location_raw
    , province
    , county
    , city
    , district
    , neighbourhood
    , no_rooms
    , price
    , area
    , offer_first_seen
    , offer_last_seen
    , offer_days_total
    , row_actv_flg
    , price_start
    , price_end
)
SELECT
    o.offer_source
    , o.offer_source_id
    , o.offer_type
    , o.offer_title
    , o.offer_url
    , o.offer_location_raw
    , o.province
    , o.county
    , o.city
    , o.district
    , o.neighbourhood
    , o.no_rooms
    , wrk.price
    , o.area
    , o.offer_first_seen
    , wrk.ds AS offer_last_seen
    , (wrk.ds - o.offer_first_seen) + 1 AS offer_days_total
    , True AS row_actv_flg
    , wrk.ds AS price_start
    , '9999-12-31'::DATE AS price_end
FROM
    wrk_{scraper_id} wrk
INNER JOIN
    offers o
ON
    o.sk_offer = wrk.sk_offer
WHERE
    etl_action = 'SCD2'
;

-- empty out working table
TRUNCATE TABLE wrk_{scraper_id};

-- updated tracker as data is completly loaded into offers table now
UPDATE etl_tracker e
SET dwh_loaded = True
    , dwh_load_ts = now()::timestamp
WHERE
    table_name = 'stg_{scraper_id}'
    AND ds = '{ds}'
;