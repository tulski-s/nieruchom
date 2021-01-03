
-- Monthly rent offer price in selected cities

\set MONTH 11;

WITH base AS (
  SELECT
    *
    , ROW_NUMBER() OVER (PARTITION BY offer_source_id ORDER BY price_end DESC) AS rn
    , EXTRACT(MONTH FROM offer_first_seen) AS month_first_seen
    , EXTRACT(MONTH FROM offer_last_seen) AS month_last_seen
    , CASE
        WHEN area <= 38 THEN '0-38'
        WHEN area > 38 AND area <= 60 THEN '38-60'
        WHEN area > 60 AND area <= 90 THEN '60-90'
      END AS area_category
  FROM
    offers
  WHERE
    -- this will output all offers that were seen in given month
    :MONTH BETWEEN EXTRACT(MONTH FROM offer_first_seen) AND EXTRACT(MONTH FROM offer_last_seen)
    -- now I need prices changes up to end of given month (changes can start earlier months though)
    AND EXTRACT(MONTH FROM price_start) <= :MONTH
    AND city IN (
      'bydgoszcz', 'gdańsk', 'katowice', 'kraków', 'lublin', 'lódź', 'poznań', 'szczecin', 'warszawa', 'wrocław'
    )
    -- look at rent only
    AND offer_type = 'rent'
)
SELECT
  city
  , area_category
  , COUNT(1) AS no_offers
  , MAX(price) AS max_price
  , MIN(price) AS min_price
  , ROUND(AVG(price), 0) AS avg_price
FROM
  base
WHERE
  rn = 1
  AND area_category IS NOT NULL
GROUP BY
  1, 2
;