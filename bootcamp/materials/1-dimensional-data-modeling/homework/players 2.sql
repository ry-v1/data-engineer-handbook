--INSERT INTO players

WITH years AS (
    SELECT *
    FROM GENERATE_SERIES(1970, 2021) AS current_year
), p AS (
    SELECT
        actor, actorid
        , MIN(year) AS first_year
    FROM actor_films
    --WHERE actor = 'Orson Welles'
    GROUP BY actor, actorid
), players_and_seasons AS (
    SELECT *
    FROM p
    JOIN years y
        ON p.first_year <= y.current_year
) 
, windowed AS (
    SELECT DISTINCT
        pas.actor, pas.actorid, pas.current_year
        , ps.year
        , ARRAY_REMOVE(
            ARRAY_AGG(
                CASE
                    WHEN ps.year IS NOT NULL
                    THEN
                     ROW(
                        ps.film,
                        ps.year,
                        ps.votes,
                        ps.rating,
                        ps.filmid)::films_type                    
                END
                )
            OVER (PARTITION BY pas.actor, pas.actorid ORDER BY COALESCE(pas.current_year, ps.year)),
           NULL
        )  AS films
        , ROUND(AVG(rating) OVER (PARTITION BY pas.actor, pas.actorid ORDER BY COALESCE(pas.current_year, ps.year))) AS avg_rating
    FROM players_and_seasons pas
    LEFT JOIN actor_films ps
        ON pas.actor = ps.actor
        AND pas.actorid = ps.actorid
        AND pas.current_year = ps.year
 )
SELECT
    actor, actorid, current_year, films--, avg_rating
    , CASE
        WHEN avg_rating > 8 THEN 'star'
        WHEN avg_rating > 7 THEN 'good'
        WHEN avg_rating > 6 THEN 'average'
        ELSE 'bad'
    END::scoring_class AS scoring_class
FROM windowed w
;

/* ), agg AS (
    SELECT 
        actor, actorid, year
        , array_agg(ARRAY[ROW(
                film,
                year,
                votes,
                rating,
                filmid )::films_type]) AS films
    , ROUND(AVG(rating)) AS avg_rating
    FROM actor_films
    WHERE actor = 'Orson Welles'
    GROUP BY actor, actorid, year  */

Orson Welles	nm0000080	1970	{{"(Catch-22,1970,22272,7.1,tt0065528)"},{"(\"The Deep\",1970,223,6.7,tt0209994)"},{"(\"The Kremlin Letter\",1970,1845,6.3,tt0065950)"},{"(\"Start the Revolution Without Me\",1970,2650,6.5,tt0066402)"},{"(Waterloo,1970,9106,7.3,tt0066549)"}}	7
2	Orson Welles	nm0000080	1971	{{"(\"A Safe Place\",1971,1002,5.2,tt0067699)"},{"(\"Ten Days Wonder\",1971,773,6.2,tt0068521)"},{"(Malpertuis,1971,1324,6.9,tt0067386)"}}	6
3	Orson Welles	nm0000080	1972	{{"(\"Treasure Island\",1972,1165,5.9,tt0069229)"},{"(\"Get to Know Your Rabbit\",1972,659,5.5,tt0068637)"},{"(Necromancy,1972,792,4.7,tt0068994)"}}	5
4	Orson Welles	nm0000080	1973	{{"(\"The Battle of Sutjeska\",1973,1468,6.9,tt0070758)"}}	7
5	Orson Welles	nm0000080	1974	{{"(\"Ten Little Indians\",1974,2809,5.7,tt0072263)"}}	6
6	Orson Welles	nm0000080	1976	{{"(\"Voyage of the Damned\",1976,2670,6.5,tt0075406)"}}	6
7	Orson Welles	nm0000080	1978	{{"(\"The Biggest Battle\",1978,436,4.9,tt0076102)"}}	5
8	Orson Welles	nm0000080	1979	{{"(\"The Double McGuffin\",1979,367,6.6,tt0079070)"},{"(\"The Muppet Movie\",1979,33166,7.6,tt0079588)"}}	7
9	Orson Welles	nm0000080	1980	{{"(\"The Secret Life of Nikola Tesla\",1980,1283,7.4,tt0079985)"}}	7
10	Orson Welles	nm0000080	1981	{{"(Butterfly,1981,1219,4.7,tt0082122)"},{"(\"The Enchanted Journey\",1981,117,6.6,tt0087202)"},{"(\"The Man Who Saw Tomorrow\",1981,1016,6.2,tt0081109)"},{"(\"History of the World: Part I\",1981,46199,6.9,tt0082517)"}}	6
11	Orson Welles	nm0000080	1982	{{"(\"Slapstick of Another Kind\",1982,750,2.6,tt0088134)"}}	3
12	Orson Welles	nm0000080	1984	{{"(\"Where Is Parsifal?\",1984,103,5.1,tt0086577)"}}	5
13	Orson Welles	nm0000080	1986	{{"(\"The Transformers: The Movie\",1986,37646,7.3,tt0092106)"}}	7
14	Orson Welles	nm0000080	1987	{{"(\"Someone to Love\",1987,360,5.9,tt0094007)"}}	6