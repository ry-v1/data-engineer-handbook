/*
    - A cumulative query to generate `device_activity_datelist` from `events`
*/

/*
SELECT min(event_time), max(event_time) FROM events LIMIT 10;
SELECT * FROM events LIMIT 10;
SELECT * FROM devices LIMIT 10; WHERE user_id = '70132547320211180';

TRUNCATE TABLE user_devices_cumulated
;
    SELECT * 
        FROM events e
        JOIN devices d
        ON e.device_id = d.device_id 
*/

    INSERT INTO user_devices_cumulated
    WITH yesterday AS (
        SELECT 
            *
        FROM user_devices_cumulated
        WHERE date = '2023-01-01'
    ), today AS (
        SELECT 
            CAST(e.user_id AS TEXT) AS user_id
            , d.browser_type
            , MAX(DATE(CAST(e.event_time AS TIMESTAMP))) AS date_active
        FROM events e
        JOIN devices d
        ON e.device_id = d.device_id 
        WHERE DATE(CAST(e.event_time AS TIMESTAMP)) = '2023-01-02'::date 
            AND e.user_id IS NOT NULL
            and d.browser_type IS NOT NULL
            --AND user_id = '743774307695414700	'
        GROUP BY 1, 2
    )
    SELECT 
        COALESCE (t.user_id, y.user_id) AS user_id
        , COALESCE (t.browser_type, y.browser_type) AS browser_type
        , CASE
            WHEN y.device_activity_datelist IS NULL THEN ARRAY[t.date_active]
            WHEN t.date_active IS NULL THEN y.device_activity_datelist
            ELSE ARRAY[t.date_active] || y.device_activity_datelist
        END AS dates_active
        , COALESCE (t.date_active, y.date + INTERVAL '1 day') AS date
    FROM today t
    FULL OUTER JOIN yesterday y
    ON t.user_id = y.user_id
    ;

 /*    SELECT * FROM user_devices_cumulated 
    ORDER BY user_id, date */

with yesterday as(
select * 
from user_devices_cumulated 
where date= date('2023-01-02')
and user_id ='9446887345398050000'
),
today as (
select  cast(e.user_id as text) as user_id,
        d.browser_type as browser_type,
        max(date(cast(e.event_time as timestamp))) as device_activity_date
from events e join devices d
on e.device_id = d.device_id 
where  date(cast(e.event_time as timestamp))= date('2023-01-03')
        and e.user_id is not null 
        and d.browser_type is not null
        and user_id ='9446887345398050000'
group by e.user_id, d.browser_type
)
select coalesce(t.user_id, y.user_id) as user_id,
coalesce(t.browser_type, y.browser_type) as browser_type,
Case when y.device_activity_datelist is null then array[t.device_activity_date]
    when t.device_activity_date is null then y.device_activity_datelist
    else array[t.device_activity_date] || y.device_activity_datelist
End
as device_activity_datelist,
coalesce(t.device_activity_date, y.date + Interval '1 Day') as date
from today as t full outer join yesterday as y
on t.user_id = y.user_id 
where t.user_id ='9446887345398050000'

with deduped_devices as (
select device_id,browser_type, 
        row_number() over(partition by device_id,browser_type) as rn
from devices
), --select * from deduped_devices where rn = 1 and browser_type = 'Googlebot'
events_deduped as (
select cast(user_id as text) as user_id,device_id, date(cast(event_time as timestamp)) event_time,
        row_number() over(partition by cast(user_id as text),device_id order by date(cast(event_time as timestamp)) desc) as rn
from     events  where user_id =  '9446887345398050000'
),
yesterday as(
select * 
from user_devices_cumulated 
where date= date('2023-01-02')
and user_id ='9446887345398050000'

)
,today as (
select  e.user_id as user_id,
        d.browser_type as browser_type,
        max(date(e.event_time)) as device_activity_date
from events_deduped e join deduped_devices d
on e.device_id = d.device_id 
where d.rn = 1 and --e.rn = 1 and 
date(e.event_time)= date('2023-01-03')
        and e.user_id is not null
        and d.browser_type is not null
        and user_id ='9446887345398050000'

group by e.user_id, d.browser_type 
)
select  coalesce(t.user_id, y.user_id) as user_id,
        coalesce(t.browser_type, y.browser_type) as browser_type,
        Case when y.device_activity_datelist is null then array[t.device_activity_date]
             when t.device_activity_date is null then y.device_activity_datelist
             else array[t.device_activity_date] || y.device_activity_datelist
        End
        as device_activity_datelist,
        coalesce(t.device_activity_date, y.date + Interval '1 Day') as date
from today as t full outer join yesterday as y
on t.user_id = y.user_id
where t.user_id ='9446887345398050000'



with deduped_devices as (
select e.device_id,browser_type, 
        row_number() over(partition by cast(user_id as text),e.device_id,browser_type, date(cast(event_time as timestamp)) order by date(cast(event_time as timestamp)) desc) as rn
, cast(user_id as text) as user_id, date(cast(event_time as timestamp)) event_time
from     events e
join devices d
on e.device_id = d.device_id 
where e.user_id is not null
        and d.browser_type is not null
  and user_id =  '9446887345398050000'
),
yesterday as(
select * 
from user_devices_cumulated 
where date= date('2023-01-02')
and user_id ='9446887345398050000'

)
,today as (
select  d.user_id as user_id,
        d.browser_type as browser_type,
        (date(d.event_time)) as device_activity_date
from deduped_devices d
where d.rn = 1 and --e.rn = 1 and 
date(d.event_time)= date('2023-01-03')
        and user_id ='9446887345398050000'
--group by e.user_id, d.browser_type 
)
select  coalesce(t.user_id, y.user_id) as user_id,
        coalesce(t.browser_type, y.browser_type) as browser_type,
        Case when y.device_activity_datelist is null then array[t.device_activity_date]
             when t.device_activity_date is null then y.device_activity_datelist
             else array[t.device_activity_date] || y.device_activity_datelist
        End
        as device_activity_datelist,
        coalesce(t.device_activity_date, y.date + Interval '1 Day') as date
from today as t full outer join yesterday as y
on t.user_id = y.user_id
where t.user_id ='9446887345398050000'
