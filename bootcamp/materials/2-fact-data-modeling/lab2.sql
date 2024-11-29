-- SELECT * FROM events LIMIT 10;

-- DROP TABLE IF EXISTS users_cumulated
-- ;

-- CREATE TABLE users_cumulated(
--     user_id TEXT
--     , dates_active DATE[]
--     , date DATE
--     , PRIMARY KEY (user_id, date)
--     )
-- ;

INSERT INTO users_cumulated
WITH yesterday AS (
    SELECT *
    FROM users_cumulated
    WHERE date = DATE('2023-01-30')
    )
,   
    today AS (
        SELECT
            CAST(user_id AS TEXT) AS user_id
            , DATE(CAST(event_time AS TIMESTAMP)) AS date_active
        FROM events
        WHERE DATE(CAST(event_time AS TIMESTAMP)) = DATE('2023-01-31')
            AND user_id IS NOT NULL
        GROUP BY user_id, DATE(CAST(event_time AS TIMESTAMP))
    )

SELECT 
    COALESCE(t.user_id, y.user_id) AS user_id
    , CASE WHEN y.dates_active IS NULL
        THEN ARRAY[t.date_active]
        ELSE y.dates_active || ARRAY[t.date_active]
        END AS dates_active
    , COALESCE(t.date_active, y.date + INTERVAL '1 DAY') AS date
FROM today t
FULL OUTER JOIN yesterday y
ON t.user_id = y.user_id
;

SELECT * FROM users_cumulated where date = '2023-01-31'
;

SELECT * FROM GENERATE_SERIES( DATE('2023-01-01'), DATE('2023-01-31'), INTERVAL '1 DAY')
;

WITH users AS (
    SELECT *
    FROM users_cumulated
    WHERE date = DATE('2023-01-31')
    )
,
    series AS (
        SELECT *
        FROM GENERATE_SERIES( DATE('2023-01-01'), DATE('2023-01-31'), INTERVAL '1 DAY') AS series_date
    )
,
    placeholder_ints AS(
        SELECT
            CASE WHEN
                dates_active @> ARRAY[DATE(series_date)]
                THEN CAST(POW(2, 31 - (date - DATE(series_date))) AS BIGINT)
                ELSE 0
                END AS placeholder_int_value
            , *
        FROM users 
        CROSS JOIN series       
        WHERE user_id = '6499095000191390000' --'439578290726747300'
    )
    SELECT 
        user_id
        , CAST(CAST(SUM(placeholder_int_value) AS BIGINT) AS BIT(32))
        , BIT_COUNT(CAST(CAST(SUM(placeholder_int_value) AS BIGINT) AS BIT(32))) > 0
            AS dim_is_monthly_active
        , BIT_COUNT(CAST('11111110000000000000000000000000' AS BIT(32)) &
            CAST(CAST(SUM(placeholder_int_value) AS BIGINT) AS BIT(32))) > 0
            AS dim_is_weekly_active
    FROM placeholder_ints
    GROUP BY user_id


    SELECT *
    FROM users_cumulated
    WHERE user_id = '6499095000191390000' --'439578290726747300'

2023-01-03,2023-01-04,,,,,2023-01-09,,2023-01-11,,,,,2023-01-16,,,,,,,,,,,,
2023-01-28,2023-01-29,,2023-01-31

6499095000191390000	10110000000000010000101000011000	8
6499095000191390000	01100000000000100001010000110000	7

11111111100000000000000000000000
01111111110000000000000000000000

SELECT CAST(POW(2, 32) AS BIGINT) -- 4294967296 
;
SELECT CAST(POW(2, 31) AS BIGINT) -- 2147483648
;
SELECT DATE(GENERATE_SERIES( DATE('2023-01-01'), DATE('2023-01-31')
    , INTERVAL '1 DAY'))
;
SELECT ('2023-01-02' 
            - DATE(GENERATE_SERIES( DATE('2023-01-01'), DATE('2023-01-31')
                , INTERVAL '1 DAY')))
;
SELECT 31 - ('2023-01-01' 
            - DATE(GENERATE_SERIES( DATE('2023-01-01'), DATE('2023-01-31')
                , INTERVAL '1 DAY')))
;
SELECT 
CAST(POW(2
    , 31 - ('2023-01-01' 
            - DATE(GENERATE_SERIES( DATE('2023-01-01'), DATE('2023-01-31')
                , INTERVAL '1 DAY')))) AS BIGINT)
;
SELECT BIT_COUNT(
    CAST(CAST(POW(2 , 31 - ('2023-01-02' 
            - DATE(GENERATE_SERIES( DATE('2023-01-01'), DATE('2023-01-31')
                , INTERVAL '1 DAY')))) AS BIGINT) AS BIT(32)))
/*
anyarray @> anyarray → boolean

Does the first array contain the second, that is, does each element appearing in the second array equal some element of the first array? (Duplicates are not treated specially, thus ARRAY[1] and ARRAY[1,1] are each considered to contain the other.)

ARRAY[1,4,3] @> ARRAY[3,1,3] → t
*/
