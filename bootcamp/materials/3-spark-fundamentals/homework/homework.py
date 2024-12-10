from pyspark.sql import SparkSession
from pyspark.sql.functions import expr, col, lit, broadcast, avg, count
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
            .withColumnRenamed('description', 'map_description') \
            .withColumnRenamed('mapid', 'mapid')

df_medals_maps_broadcast = df_medals.join(broadcast(df_maps)) 

df_medals_maps_broadcast.show()

#   - Bucket join `match_details`, `matches`, and `medal_matches_players` on `match_id` with `16` buckets

df_match_details = spark.read.option("header", "true") \
            .csv("/home/iceberg/data/match_details.csv")

df_matches = spark.read.option("header", "true") \
            .csv("/home/iceberg/data/matches.csv")

df_medal_matches_players = spark.read.option("header", "true") \
            .csv("/home/iceberg/data/medals_matches_players.csv")

spark.sql(""" CREATE DATABASE IF NOT EXISTS bootcamp""")

num_buckets = 16

spark.sql(""" drop table if exists bootcamp.match_details_bucketed """)

bucketed_match_details_DDL = """
CREATE TABLE IF NOT EXISTS bootcamp.match_details_bucketed (
    match_id STRING
    ,player_gamertag STRING
    ,previous_spartan_rank int
    ,spartan_rank int
    ,previous_total_xp int
    ,total_xp int
    ,previous_csr_tier int
    ,previous_csr_designation int
    ,previous_csr int
    ,previous_csr_percent_to_next_tier int
    ,previous_csr_rank int
    ,current_csr_tier int
    ,current_csr_designation int
    ,current_csr int
    ,current_csr_percent_to_next_tier int
    ,current_csr_rank int
    ,player_rank_on_team int
    ,player_finished boolean
    ,player_average_life int
    ,player_total_kills int
    ,player_total_headshots int
    ,player_total_weapon_damage int
    ,player_total_shots_landed int
    ,player_total_melee_kills int
    ,player_total_melee_damage int
    ,player_total_assassinations int
    ,player_total_ground_pound_kills int
    ,player_total_shoulder_bash_kills int
    ,player_total_grenade_damage int
    ,player_total_power_weapon_damage int
    ,player_total_power_weapon_grabs int
    ,player_total_deaths int
    ,player_total_assists int
    ,player_total_grenade_kills int
    ,did_win int
    ,team_id int
)
USING iceberg
PARTITIONED BY (bucket(16, match_id));
"""
spark.sql(bucketed_match_details_DDL)

df_match_details.write.bucketBy(num_buckets, "match_id").saveAsTable("bootcamp.match_details_bucketed", format="parquet", mode="overwrite")


spark.sql(""" drop table if exists bootcamp.matches_bucketed """)

bucketed_matches_DDL = """
CREATE TABLE IF NOT EXISTS bootcamp.matches_bucketed (
    match_id STRING
    ,mapid STRING
    ,is_team_game boolean
    ,playlist_id STRING
    ,game_variant_id STRING
    ,is_match_over boolean
    ,completion_date timestamp
    ,match_duration STRING
    ,game_mode STRING
    ,map_variant_id STRING
)
USING iceberg
PARTITIONED BY (bucket(16, match_id));
"""
spark.sql(bucketed_matches_DDL)

df_matches.write.bucketBy(16, "match_id").saveAsTable("bootcamp.matches_bucketed", format="parquet", mode="overwrite")

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

# create buckted tables

bucketed_match_details = spark.table("bootcamp.match_details_bucketed")
bucketed_matches = spark.table("bootcamp.matches_bucketed")
bucketed_medal_matches_players = spark.table("bootcamp.medal_matches_players_bucketed")


#   - Aggregate the joined data frame to figure out questions like:

# join the bucketed tables
result = (bucketed_match_details.join(bucketed_matches, "match_id")
            .join(bucketed_medal_matches_players, "match_id")
            .join(df_medals_maps_broadcast, "mapid")          
            .select(bucketed_match_details["*"], bucketed_matches["mapid"], bucketed_matches["is_team_game"], bucketed_matches["playlist_id"]
                , bucketed_matches["game_variant_id"], bucketed_matches["is_match_over"], bucketed_matches["completion_date"]
                , bucketed_matches["match_duration"], bucketed_matches["game_mode"], bucketed_matches["map_variant_id"]
                , bucketed_medal_matches_players["medal_id"], bucketed_medal_matches_players["count"]
                , df_medals_maps_broadcast["map_name"], df_medals_maps_broadcast["map_description"]
                , df_medals_maps_broadcast["classification"], df_medals_maps_broadcast["difficulty"]
                , df_medals_maps_broadcast["medals_description"], df_medals_maps_broadcast["medals_name"]
            )
         )
#     - Which player averages the most kills per game?
avg_kills = result.groupBy(["player_gamertag", "match_id"]).agg(avg("player_total_kills").alias("avg_kills"))
most_kills = avg_kills.orderBy("avg_kills", ascending=False).limit(1)
most_kills.show()
'''
>>>
+---------------+--------------------+---------+
|player_gamertag|            match_id|avg_kills|
+---------------+--------------------+---------+
|   gimpinator14|acf0e47e-20ac-4b1...|    109.0|
+---------------+--------------------+---------+
'''


#     - Which playlist gets played the most?
agg_playlist = result.groupBy(["playlist_id"]).agg(countDistinct("match_id").alias("agg_playlist_id"))
most_played = agg_playlist.orderBy("agg_playlist_id", ascending=False).limit(1)
most_played.select("agg_playlist_id").show()
'''
>>>
+--------------------+---------------+
|         playlist_id|agg_playlist_id|
+--------------------+---------------+
|f72e0ef0-7c4a-430...|           7640|
+--------------------+---------------+
'''


#     - Which map gets played the most?
agg_map_id = result.groupBy(["mapid", "map_name"]).agg(countDistinct("match_id").alias("agg_mapid"))
most_map = agg_map_id.orderBy("agg_mapid", ascending=False).limit(1)
most_map.select("agg_mapid").show()
'''
>>>
+--------------------+--------------+---------+
|               mapid|      map_name|agg_mapid|
+--------------------+--------------+---------+
|c7edbf0f-f206-11e...|Breakout Arena|     7032|
+--------------------+--------------+---------+
'''


#     - Which map do players get the most Killing Spree medals on?
medal_map = (result.filter("classification == 'KillingSpree'")
             .groupBy(["mapid", "map_name"]).agg(countDistinct("match_id").alias("map_kills"))
             .select("map_name", "mapid","map_kills"))
most_kills_map = medal_map.orderBy("map_kills", ascending=False).limit(1)
most_kills_map.select("map_kills").show()
'''
>>>
+--------------+--------------------+---------+
|      map_name|               mapid|map_kills|
+--------------+--------------------+---------+
|Breakout Arena|c7edbf0f-f206-11e...|     4917|
+--------------+--------------------+---------+
'''


#   - With the aggregated data set
#     - Try different `.sortWithinPartitions` to see which has the smallest data size (hint: playlists and maps are both very low cardinality)

# without any partition
result.write.mode("overwrite").saveAsTable("bootcamp.resultUnsorted")

# with partition by playlist_id
result.sortWithinPartitions("playlist_id").write.mode("append").saveAsTable("bootcamp.sorted_by_playlist_id")

# with partition by mapid
result.sortWithinPartitions("mapid").write.mode("append").saveAsTable("bootcamp.sorted_by_mapid")

spark.sql("""
            SELECT
                    SUM(file_size_in_bytes) as total_size
                ,   COUNT(1) AS number_of_files
                ,   "no sorting"
            FROM bootcamp.resultUnsorted.files
            UNION ALL
            SELECT
                    SUM(file_size_in_bytes) as total_size
                ,   COUNT(1) AS number_of_files
                ,   "sorted_by_playlist_id"
            FROM bootcamp.sorted_by_playlist_id.files
            UNION ALL
            SELECT
                    SUM(file_size_in_bytes) as total_size
                ,   COUNT(1) AS number_of_files
                ,   "sorted_by_mapid"
            FROM bootcamp.sorted_by_mapid.files
            """
)
'''
>>>
total_size	number_of_files	no sorting
151626803	20	no sorting
147392783	20	sorted_by_playlist_id
150102931	20	sorted_by_mapid
'''
