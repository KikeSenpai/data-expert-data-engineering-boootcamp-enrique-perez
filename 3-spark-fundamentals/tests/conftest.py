import pytest

import pyspark.sql.types as T

from datetime import date
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark_fixture():
    builder = (
        SparkSession.builder.appName("TestingPySparkTransformations")  # pyright: ignore[reportAttributeAccessIssue]
        .master("local[*]")
        .config("spark.driver.host", "localhost")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", 1)
        .config("spark.default.parallelism", 1)
        .config("spark.sql.execution.arrow.pyspark.enabled", True)
    )
    spark = builder.getOrCreate()
    yield spark
    spark.stop()


@pytest.fixture(scope="session")
def activity_datelist_df(spark_fixture):
    data = [
        {"user_id": 1037710827, "device_activity_datelist": [{"browser_type": "Firefox", "datelist": [date(2023, 1, 1)]}], "last_updated_at": date(2023, 1, 1)},
        {"user_id": 1180485268, "device_activity_datelist": [{"browser_type": "Firefox", "datelist": [date(2023, 1, 1)]}], "last_updated_at": date(2023, 1, 1)},
        {"user_id": 747494706, "device_activity_datelist": [{"browser_type": "Firefox", "datelist": [date(2023, 1, 1)]}], "last_updated_at": date(2023, 1, 1)},
        {"user_id": 824540328, "device_activity_datelist": [{"browser_type": "Firefox", "datelist": [date(2023, 1, 1)]}], "last_updated_at": date(2023, 1, 1)},
        {"user_id": 1833036683, "device_activity_datelist": [{"browser_type": "Firefox", "datelist": [date(2023, 1, 1)]}], "last_updated_at": date(2023, 1, 1)},
    ]

    schema = T.StructType(
        [
            T.StructField("user_id", T.IntegerType(), True),
            T.StructField(
                "device_activity_datelist",
                T.ArrayType(T.StructType(
                    [
                        T.StructField("browser_type", T.StringType(), True),
                        T.StructField("datelist", T.ArrayType(T.DateType()), True),
                    ]
                )), True),
            T.StructField("last_updated_at", T.DateType(), True),
        ]
    )

    return spark_fixture.createDataFrame(data, schema=schema)
