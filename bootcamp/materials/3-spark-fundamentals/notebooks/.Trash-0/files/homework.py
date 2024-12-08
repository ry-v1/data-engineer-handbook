from pyspark.sql import SparkSession
from pyspark.sql.functions import expr, col, lit, broadcast
spark = SparkSession.builder.appName("homework").getOrCreate()

#  - Disabled automatic broadcast join with `spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")`
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")

#   - Explicitly broadcast JOINs `medals` and `maps`


df_medals = spark.read.option("header", "true") \
            .csv("/home/iceberg/data/medals.csv") \
            .withColumnRenamed('name', 'medals_name') \
            .withColumnRenamed('description', 'medals_description')    
        
df_maps = spark.read.option("header", "true") \
            .csv("/home/iceberg/data/maps.csv") \
            .withColumnRenamed('name', 'map_name') \
            .withColumnRenamed('description', 'map_description')
#df_maps.show()

df_medals_maps_broadcast = df_medals.join(broadcast(df_maps))

df_medals_maps_broadcast.show()

#   - Bucket join `match_details`, `matches`, and `medal_matches_players` on `match_id` with `16` buckets

df_match_details = spark.read.option("header", "true") \
            .csv("/home/iceberg/data/match_details.csv")

df_matches = spark.read.option("header", "true") \
            .csv("/home/iceberg/data/medals.csv")

df_medal_matches_players = spark.read.option("header", "true") \
            .csv("/home/iceberg/data/medals_matches_players.csv")

# Write df1 with bucketing
# df_match_details.write.bucketBy(16, "match_id").saveAsTable("bucketed_match_details")
# df_matches.write.bucketBy(16, "match_id").saveAsTable("bucketed_matches")
# df_medal_matches_players.write.bucketBy(16, "match_id").saveAsTable("bucketed_medal_matches_players")
spark.sql(""" CREATE DATABASE IF NOT EXISTS bootcamp""")


num_buckets = 16

spark.sql(""" drop table if exists bootcamp.match_details_bucketed """)

bucketed_match_details_DDL = """
CREATE TABLE IF NOT EXISTS bootcamp.match_details_bucketed (
    match_id STRING
    ,player_gamertag STRING
    ,previous_spartan_rank STRING
    ,spartan_rank STRING
    ,previous_total_xp STRING
    ,total_xp STRING
    ,previous_csr_tier STRING
    ,previous_csr_designation STRING
    ,previous_csr STRING
    ,previous_csr_percent_to_next_tier STRING
    ,previous_csr_rank STRING
    ,current_csr_tier STRING
    ,current_csr_designation STRING
    ,current_csr STRING
    ,current_csr_percent_to_next_tier STRING
    ,current_csr_rank STRING
    ,player_rank_on_team STRING
    ,player_finished STRING
    ,player_average_life STRING
    ,player_total_kills STRING
    ,player_total_headshots STRING
    ,player_total_weapon_damage STRING
    ,player_total_shots_landed STRING
    ,player_total_melee_kills STRING
    ,player_total_melee_damage STRING
    ,player_total_assassinations STRING
    ,player_total_ground_pound_kills STRING
    ,player_total_shoulder_bash_kills STRING
    ,player_total_grenade_damage STRING
    ,player_total_power_weapon_damage STRING
    ,player_total_power_weapon_grabs STRING
    ,player_total_deaths STRING
    ,player_total_assists STRING
    ,player_total_grenade_kills STRING
    ,did_win STRING
    ,team_id STRING
)
USING iceberg
PARTITIONED BY (bucket(16, match_id));
"""
spark.sql(bucketed_match_details_DDL)


df_match_details.write.bucketBy(num_buckets, "match_id").saveAsTable("bootcamp.match_details_bucketed", format="parquet", mode="overwrite")

spark.sql(""" drop table if exists bootcamp.matches_bucketed """)

bucketed_matches_DDL = """
CREATE TABLE IF NOT EXISTS bootcamp.matches_bucketed2 (
    match_id STRING
    ,mapid STRING
    ,is_team_game STRING
    ,playlist_id STRING
    ,game_variant_id STRING
    ,is_match_over STRING
    ,completion_date STRING
    ,match_duration STRING
    ,game_mode STRING
    ,map_variant_id STRING
)
USING iceberg
PARTITIONED BY (bucket(16, match_id));
"""
spark.sql(bucketed_matches_DDL)

df_matches.write.bucketBy(16, "match_id").saveAsTable("bootcamp.matches_bucketed2", format="parquet", mode="overwrite")

spark.sql(""" drop table if exists bootcamp.medal_matches_players_bucketed """)

bucketed_medal_matches_players_DDL = """
CREATE TABLE IF NOT EXISTS bootcamp.medal_matches_players_bucketed (
   match_id STRING,
   player_gamertag STRING,
   medal_id INTEGER,
   count INTEGER
)
USING iceberg
PARTITIONED BY (bucket(16, match_id));
"""
spark.sql(bucketed_medal_matches_players_DDL)

df_medal_matches_players.write.bucketBy(num_buckets, "match_id").saveAsTable("bootcamp.medal_matches_players_bucketed", format="parquet", mode="overwrite")


spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")

spark.sql("""select * from bootcamp.match_details_bucketed limit 10; """)

bucketed_match_details = spark.table("bootcamp.match_details_bucketed")
bucketed_matches = spark.table("bootcamp.matches_bucketed")
bucketed_medal_matches_players = spark.table("bootcamp.medal_matches_players_bucketed")

# Read df2 and join with bucketed df1
result = (bucketed_match_details.join(bucketed_matches, "match_id")
        .join(bucketed_medal_matches_players, "match_id"))

        # .join(matchDetailsBucketTable, Seq("match_id"))
        # .join(medalMatchesPlayersExpandedBucketTable, Seq("match_id"), "left")


#   - Aggregate the joined data frame to figure out questions like:
#     - Which player averages the most kills per game?
#     - Which playlist gets played the most?
#     - Which map gets played the most?
#     - Which map do players get the most Killing Spree medals on?
#   - With the aggregated data set
#     - Try different `.sortWithinPartitions` to see which has the smallest data size (hint: playlists and maps are both very low cardinality)