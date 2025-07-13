# Spark Fundamentals Week

## Data documentation

- `match_details.csv`: a row for every players performance in a match.
- `matches.csv`: a row for every match.
- `medals_matches_players.csv`: a row for every medal type a player gets in a match.
- `medals.csv`: a row for every medal type.

## Instructions

Your goal is to make the following things happen:

1. Build a Spark job that:
  - Disabled automatic broadcast join with `spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")`.
  - Explicitly broadcast JOINs `medals` and `maps`.
  - Bucket join `match_details`, `matches`, and `medal_matches_players` on `match_id` with `16` buckets.
  - Aggregate the joined data frame to figure out questions like:
    - Which player averages the most kills per game?
    - Which playlist gets played the most?
    - Which map gets played the most?
    - Which map do players get the most Killing Spree medals on?
  - With the aggregated data set try different `.sortWithinPartitions` to see which has the smallest data size (hint: playlists and maps are both very low cardinality).

2. Create tests:
  - Convert 2 queries from Weeks 1-2 from PostgreSQL to SparkSQL.
  - Create new PySpark jobs for these queries.
  - Create tests with fake input and expected output data.

## Deliverables

Save these as `.py` files and submit a zip file containing them.
