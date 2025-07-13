from pyspark.sql import SparkSession, DataFrame

ddl_query = """
    CREATE TABLE IF NOT EXISTS devices.user_devices_cumulated (
        user_id BIGINT,
        device_activity_datelist
            ARRAY<STRUCT<
                browser_type: STRING,
                datelist: ARRAY<DATE>
            >>,
        last_updated_at DATE
    )
"""

population_query = """
    WITH initial_events AS (
        SELECT
            user_id,
            device_id,
            CAST(event_time AS DATE) AS event_date
        FROM events
        WHERE
            TRUE
            AND user_id IS NOT NULL
            AND CAST(event_time AS DATE) = '2023-01-01'
    ),

    enriched_events AS (
        SELECT
            e.user_id,
            d.browser_type,
            ARRAY_AGG(DISTINCT e.event_date) AS active_datelist
        FROM initial_events AS e
        LEFT JOIN devices AS d
            ON e.device_id = d.device_id
        GROUP BY 1, 2
    ),

    final_cte AS (
        SELECT
            user_id,
            ARRAY_AGG(STRUCT(
                browser_type AS browser_type,
                active_datelist AS datelist
            )) as device_activity_datelist
        FROM enriched_events
        GROUP BY 1
    )

    SELECT
        user_id,
        device_activity_datelist,
        CAST('2023-01-01' AS DATE) AS last_updated_at
    FROM final_cte
"""


def setup_tables(spark: SparkSession) -> None:
    spark.sql("CREATE DATABASE IF NOT EXISTS devices")
    spark.sql(ddl_query)

    events_df = spark.read.option("header", "true").option("inferSchema", "true").csv("../data/events.csv")
    events_df.createOrReplaceTempView("events")

    devices_df = spark.read.option("header", "true").option("inferSchema", "true").csv("../data/devices.csv")
    devices_df.createOrReplaceTempView("devices")


def transform(spark: SparkSession, query: str) -> DataFrame:
    return spark.sql(query)


def main() -> None:
    spark = SparkSession.builder.appName("UserDevicesCumulated").getOrCreate()  # pyright: ignore[reportAttributeAccessIssue]
    setup_tables(spark)
    output_df = transform(spark, population_query)
    output_df.write.insertInto("devices.user_devices_cumulated")


if __name__ == "__main__":
    main()
