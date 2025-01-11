--  What is the average number of web events of a session from a user on Tech Creator?

WITH avg_ip AS (
    SELECT COUNT(1)
    FROM processed_events_sessions
    GROUP BY ip
    )

SELECT avg(count) AS avg_events
FROM avg_ip

/*
avg_events
-------------------
1.2203389830508475

*/

--  Compare results between different hosts (zachwilson.techcreator.io, zachwilson.tech, lulu.techcreator.io)

WITH avg_site AS (
    SELECT DISTINCT count(1) OVER (PARTITION BY ip, host)
        , ip
        , host
    FROM processed_events_sessions)

SELECT avg(count) AS avg_events, host 
FROM avg_site
WHERE host IN ('zachwilson.techcreator.io', 'zachwilson.tech', 'lulu.techcreator.io')
GROUP BY host

/*
avg_events | host
------------------------------------------------
1.00000000000000000000 | zachwilson.techcreator.io
1.00000000000000000000 | zachwilson.tech
1.00000000000000000000 | lulu.techcreator.io
*/
