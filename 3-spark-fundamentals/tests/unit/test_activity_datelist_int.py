import pytest
import pyspark.sql.types as T
from datetime import date

from pyspark.testing.utils import assertDataFrameEqual

from src.jobs.activity_datelist_int import transform, transformation_query


@pytest.fixture
def expected_df(spark_fixture):
    data = [
        {"user_id": 1037710827, "device_activity_datelist_int": [{"browser_type": "Firefox", "datelist_int": 1}], "last_updated_at": date(2023, 1, 31)},
        {"user_id": 1180485268, "device_activity_datelist_int": [{"browser_type": "Firefox", "datelist_int": 1}], "last_updated_at": date(2023, 1, 31)},
        {"user_id": 747494706, "device_activity_datelist_int": [{"browser_type": "Firefox", "datelist_int": 1}], "last_updated_at": date(2023, 1, 31)},
        {"user_id": 824540328, "device_activity_datelist_int": [{"browser_type": "Firefox", "datelist_int": 1}], "last_updated_at": date(2023, 1, 31)},
        {"user_id": 1833036683, "device_activity_datelist_int": [{"browser_type": "Firefox", "datelist_int": 1}], "last_updated_at": date(2023, 1, 31)},
    ]

    schema = T.StructType(
        [
            T.StructField("user_id", T.IntegerType(), True),
            T.StructField(
                "device_activity_datelist_int",
                T.ArrayType(T.StructType(
                    [
                        T.StructField("browser_type", T.StringType(), True),
                        T.StructField("datelist_int", T.IntegerType(), True),
                    ]
                )), True),
            T.StructField("last_updated_at", T.DateType(), True),
        ]
    )

    return spark_fixture.createDataFrame(data, schema=schema)


@pytest.mark.unit
def test_should_fail_when_transformation_does_not_match(spark_fixture, activity_datelist_df, expected_df, tmp_path):
    spark_fixture.sql("CREATE DATABASE IF NOT EXISTS devices")

    table_path = tmp_path / "user_devices_cumulated"

    activity_datelist_df.write.saveAsTable(
        "devices.user_devices_cumulated",
        format="parquet",
        mode="overwrite",
        path=str(table_path),
    )

    result_df = transform(spark_fixture, transformation_query)
    assertDataFrameEqual(result_df, expected_df)
