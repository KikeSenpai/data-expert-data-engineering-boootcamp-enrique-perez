from pyspark.sql import SparkSession
import pyspark.sql.functions as F


spark = SparkSession.builder.appName("SparkHomework").getOrCreate()  # pyright: ignore[reportAttributeAccessIssue]

spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
spark.conf.set("spark.sql.debug.maxToStringFields", 10000)
spark.conf.set("spark.sql.optimizer.bucketing.enabled", "true")

medals_df = spark.read.option("header", "true").option("inferSchema", "true").csv("./data/medals.csv")

medals_matches_players_df = spark.read.option("header", "true").option("inferSchema", "true").csv("./data/medals_matches_players.csv")

medals_matches_players_df = (
    medals_matches_players_df
    .join(
        F.broadcast(medals_df)
        .withColumnRenamed("classification", "medal_classification")
        .withColumnRenamed("description", "medal_description")
        .withColumnRenamed("name", "medal_name")
        .withColumnRenamed("difficulty", "medal_difficulty"),
        "medal_id",
        "left",
    )
)

medals_matches_players_df.explain()

maps_df = spark.read.option("header", "true").option("inferSchema", "true").csv("./data/maps.csv")

matches_df = spark.read.option("header", "true").option("inferSchema", "true").csv("./data/matches.csv")

matches_df = (
    matches_df
    .join(
        F.broadcast(maps_df)
        .withColumnRenamed("name", "map_name")
        .withColumnRenamed("description", "map_description"),
        "mapid",
        "left",
    )
)

matches_df.explain()

spark.sql("CREATE DATABASE IF NOT EXISTS bootcamp")

spark.sql(
    """
    CREATE TABLE IF NOT EXISTS bootcamp.medals_matches_players_bucketed (
        medal_id long,
        match_id string,
        player_gamertag string,
        count integer,
        sprite_uri string,
        sprite_left integer,
        sprite_top integer,
        sprite_sheet_width integer,
        sprite_sheet_height integer,
        sprite_width integer,
        sprite_height integer,
        medal_classification string,
        medal_description string,
        medal_name string,
        medal_difficulty integer
    )
    USING iceberg
    PARTITIONED BY (BUCKET(16, match_id));
    """
)

medals_matches_players_df \
    .write.mode("overwrite") \
    .bucketBy(16, "match_id") \
    .saveAsTable("bootcamp.medals_matches_players_bucketed")

match_details_df = spark.read.option("header", "true").option("inferSchema", "true").csv("./data/match_details.csv")

spark.sql(
    """
    CREATE TABLE IF NOT EXISTS bootcamp.match_details_bucketed (
        match_id string,
        player_gamertag string,
        previous_spartan_rank integer,
        spartan_rank integer,
        previous_total_xp integer,
        total_xp integer,
        previous_csr_tier integer,
        previous_csr_designation integer,
        previous_csr integer,
        previous_csr_percent_to_next_tier integer,
        previous_csr_rank integer,
        current_csr_tier integer,
        current_csr_designation integer,
        current_csr integer,
        current_csr_percent_to_next_tier integer,
        current_csr_rank integer,
        player_rank_on_team integer,
        player_finished boolean,
        player_average_life string,
        player_total_kills integer,
        player_total_headshots integer,
        player_total_weapon_damage double,
        player_total_shots_landed integer,
        player_total_melee_kills integer,
        player_total_melee_damage double,
        player_total_assassinations integer,
        player_total_ground_pound_kills integer,
        player_total_shoulder_bash_kills integer,
        player_total_grenade_damage double,
        player_total_power_weapon_damage double,
        player_total_power_weapon_grabs integer,
        player_total_deaths integer,
        player_total_assists integer,
        player_total_grenade_kills integer,
        did_win integer,
        team_id integer
    )
    USING iceberg
    PARTITIONED BY (BUCKET(16, match_id));
    """
)

match_details_df \
    .write.mode("overwrite") \
    .bucketBy(16, "match_id") \
    .saveAsTable("bootcamp.match_details_bucketed")

spark.sql(
    """
    CREATE TABLE IF NOT EXISTS bootcamp.matches_bucketed (
        mapid string,
        match_id string,
        is_team_game boolean,
        playlist_id string,
        game_variant_id string,
        is_match_over boolean,
        completion_date timestamp,
        match_duration string,
        game_mode string,
        map_variant_id string,
        map_name string,
        map_description string
    )
    USING iceberg
    PARTITIONED BY (BUCKET(16, match_id));
    """
)

matches_df \
    .write.mode("overwrite") \
    .bucketBy(16, "match_id") \
    .saveAsTable("bootcamp.matches_bucketed")

medals_matches_players_all_joined_df = spark.sql(
    """
    SELECT
        mmp.match_id,
        mmp.player_gamertag,
        mmp.medal_id,
        mmp.medal_classification,
        mmp.medal_description,
        mmp.medal_name,
        mmp.medal_difficulty,
        m.mapid AS map_id,
        m.map_name,
        m.map_description,
        m.completion_date,
        m.playlist_id,
        md.player_total_kills
    FROM
        bootcamp.medals_matches_players_bucketed AS mmp
    LEFT JOIN
        bootcamp.matches_bucketed AS m
        ON mmp.match_id = m.match_id
    LEFT JOIN
        bootcamp.match_details_bucketed AS md
        ON mmp.match_id = md.match_id;
    """
)

medals_matches_players_all_joined_df.explain()

medals_matches_players_all_joined_df.createOrReplaceTempView("medals_matches_players_all")

spark.sql(
    """
    SELECT
        player_gamertag,
        SUM(player_total_kills) AS total_kills
    FROM
        medals_matches_players_all
    GROUP BY
        player_gamertag
    ORDER BY
        total_kills DESC
    """
).show()

spark.sql(
    """
    SELECT
        playlist_id,
        COUNT(DISTINCT match_id) AS num_of_plays
    FROM
        medals_matches_players_all
    GROUP BY
        playlist_id
    ORDER BY
        num_of_plays DESC
    """
).show()

spark.sql(
    """
    SELECT
        map_id,
        map_name,
        map_description,
        COUNT(DISTINCT match_id) AS num_of_plays,
        SUM(CASE WHEN medal_name = 'Killing Spree' THEN 1 ELSE 0 END) AS num_of_killing_spree_medals
    FROM
        medals_matches_players_all
    GROUP BY
        map_id,
        map_name,
        map_description
    ORDER BY
        num_of_plays DESC
    """
).show()

spark.sql(
    """
    CREATE TABLE IF NOT EXISTS bootcamp.medals_matches_players_all (
        match_id string,
        player_gamertag string,
        medal_id long,
        medal_classification string,
        medal_description string,
        medal_name string,
        medal_difficulty integer,
        map_id string,
        map_name string,
        map_description string,
        completion_date timestamp,
        playlist_id string,
        player_total_kills integer
    )
    USING iceberg
    PARTITIONED BY (DATE(completion_date));
    """
)

medals_matches_players_all_joined_df \
    .repartition(13, F.col("completion_date")) \
    .sortWithinPartitions(F.col("match_id")) \
    .write.mode("overwrite") \
    .saveAsTable("bootcamp.medals_matches_players_all")

spark.sql(
    """
    SELECT 
        SUM(file_size_in_bytes) AS size,
        COUNT(1) AS num_files
    FROM
        bootcamp.medals_matches_players_all.files
    """
).show()
