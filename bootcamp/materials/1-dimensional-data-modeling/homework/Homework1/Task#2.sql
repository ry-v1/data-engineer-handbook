/*
    2.Cumulative table generation query: Write a query that populates the actors table one year at a time.
*/

INSERT INTO actors

WITH last_year AS (
    SELECT actor, actorid, year, films, quality_class, is_active
    FROM actors
    WHERE year = 1970

), this_year AS (
    SELECT actor, actorid, year
    , array_agg(ARRAY[ROW(
                film,
                year,
                votes,
                rating,
                filmid)::films_type]) AS films
    , ROUND(AVG(rating)) AS avg_rating
    FROM actor_films
    WHERE year = 1971
    GROUP BY actor, actorid, year
)

SELECT
        COALESCE(ls.actor, ts.actor) as actor
        , COALESCE(ls.actorid, ts.actorid) as actorid
        , COALESCE(ts.year, ls.year + 1) as year
        , COALESCE(ls.films,
            ARRAY[]::films_type[]
            ) || CASE WHEN ts.year IS NOT NULL THEN ts.films
                ELSE ARRAY[]::films_type[] END
            AS films
        , CASE
            WHEN ts.year IS NOT NULL THEN
                (CASE WHEN avg_rating > 8 THEN 'star'
                WHEN avg_rating BETWEEN 7 AND 8 THEN 'good'
                WHEN avg_rating BETWEEN 6 AND 7 THEN 'average'
                ELSE 'bad' END)::quality_class_type
            ELSE ls.quality_class
            END AS quality_class
        , ts.year IS NOT NULL AS is_active
    FROM last_year ls
    FULL OUTER JOIN this_year ts
    ON ls.actor = ts.actor
;
