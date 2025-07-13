from pyspark.sql import SparkSession, DataFrame

ddl_query = """
    CREATE TABLE IF NOT EXISTS devices.user_devices_datelist_int (
        user_id BIGINT,
        device_activity_datelist_int
            ARRAY<STRUCT<
                browser_type: STRING,
                datelist_int: INTEGER
            >>,
        last_updated_at DATE
    )
"""

transformation_query = """
    WITH exploded_browser AS (
        SELECT
            user_id,
            browser_datelist.browser_type,
            browser_datelist.datelist
        FROM devices.user_devices_cumulated
        LATERAL VIEW EXPLODE(device_activity_datelist) AS browser_datelist
    ),

    exploded_dates AS (
        SELECT
            user_id,
            browser_type,
            CASE
                WHEN elem IS NOT NULL THEN TRUE
                ELSE FALSE
            END AS is_active,
            (31 - pos) AS days_since
        FROM exploded_browser AS e
        LATERAL VIEW POSEXPLODE(datelist) AS pos, elem
    ),

    datelist_int AS (
        SELECT
            user_id,
            browser_type,
            CAST(SUM(CASE WHEN is_active THEN POW(32 - days_since, 2) ELSE 0 END) AS INTEGER) AS datelist_int,
            CAST('2023-01-31' AS DATE) AS last_updated_at
        FROM exploded_dates
        GROUP BY 1, 2
    )

    SELECT
        user_id,
        ARRAY_AGG(STRUCT(
            browser_type AS browser_type,
            datelist_int AS datelist_int
        )) AS device_activity_datelist_int,
        MAX(last_updated_at) AS last_updated_at
    FROM datelist_int
    GROUP BY 1
"""


def transform(spark: SparkSession, query: str) -> DataFrame:
    return spark.sql(query)


def main() -> None:
    spark = SparkSession.builder.appName("UserDevicesDatelistInt").getOrCreate()  # pyright: ignore[reportAttributeAccessIssue]
    spark.sql("CREATE DATABASE IF NOT EXISTS devices")
    spark.sql(ddl_query)
    output_df = transform(spark, transformation_query)
    output_df.write.insertInto("devices.user_devices_datelist_int")


if __name__ == "__main__":
    main()
