DROP TYPE IF EXISTS season_stat
;

CREATE TYPE season_stat AS (
                        season Integer
                        , gp REAL
                        , pts REAL
                        , reb REAL
                        , ast REAL
                        , rating TEXT
                    )
;

SELECT 
    player_name
    , height
    , college
    , country
    , draft_year
    , draft_round
    , draft_number 
    , array_agg(ARRAY[ROW(
            season 
            , gp 
            , pts 
            , reb 
            , ast 
            , CASE WHEN pts > 20 THEN 'star'
                WHEN pts > 15 THEN 'good'
                WHEN pts > 10 THEN 'average'
                ELSE 'bad' END                          
            )::season_stat]
        )
FROM player_seasons
WHERE player_name = 'Michael Jordan'
GROUP BY
    player_name
    , height
    , college
    , country
    , draft_year
    , draft_round
    , draft_number   
;