DROP TABLE IF EXISTS players_scd_table
;

create table players_scd_table
(
	player_name text,
	scoring_class scoring_class,
	is_active boolean,
	start_season integer,
	end_season integer
	--, current_season INTEGER
);

