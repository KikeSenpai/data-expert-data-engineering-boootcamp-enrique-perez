import pytest
import pyspark.sql.types as T
from datetime import datetime

from pyspark.testing.utils import assertDataFrameEqual

from src.jobs.activity_datelist import transform, population_query


@pytest.fixture
def input_events_df(spark_fixture):
    data = [
        {"user_id": 1037710827, "device_id": 532630305, "host": "www.zachwilson.tech", "event_time": datetime(2023, 1, 1, 17, 27, 24, 241000)},
        {"user_id": 1037710827, "device_id": 532630305, "host": "www.eczachly.com", "event_time": datetime(2023, 1, 1, 11, 26, 21, 247000)},
        {"user_id": 1180485268, "device_id": 532630305, "host": "admin.zachwilson.tech", "event_time": datetime(2023, 1, 1, 16, 19, 30, 738000)},
        {"user_id": 1180485268, "device_id": 532630305, "host": "www.zachwilson.tech", "event_time": datetime(2023, 1, 1, 15, 53, 14, 466000)},
        {"user_id": 747494706, "device_id": 532630305, "host": "www.zachwilson.tech", "event_time": datetime(2023, 1, 1, 16, 3, 17, 535000)},
        {"user_id": 747494706, "device_id": 532630305, "host": "admin.zachwilson.tech", "event_time": datetime(2023, 1, 1, 16, 8, 17, 975000)},
        {"user_id": 824540328, "device_id": 532630305, "host": "admin.zachwilson.tech", "event_time": datetime(2023, 1, 1, 17, 23, 14, 992000)},
        {"user_id": 824540328, "device_id": 532630305, "host": "www.eczachly.com", "event_time": datetime(2023, 1, 1, 1, 22, 13, 197000)},
        {"user_id": 1833036683, "device_id": 532630305, "host": "admin.zachwilson.tech", "event_time": datetime(2023, 1, 1, 3, 15, 18, 583000)},
        {"user_id": 1833036683, "device_id": 532630305, "host": "www.eczachly.com", "event_time": datetime(2023, 1, 1, 0, 3, 0, 624000)},
        {"user_id": 1809929467, "device_id": 906264142, "host": "admin.zachwilson.tech", "event_time": datetime(2021, 2, 22, 1, 36, 52, 420000)},
        {"user_id": 2002285749, "device_id": 906264142, "host": "www.eczachly.com", "event_time": datetime(2021, 2, 22, 2, 25, 41, 865000)},
        {"user_id": 1562965412, "device_id": 906264142, "host": "www.zachwilson.tech", "event_time": datetime(2021, 1, 30, 20, 46, 3, 961000)},
    ]

    schema = T.StructType(
        [
            T.StructField("user_id", T.IntegerType(), True),
            T.StructField("device_id", T.IntegerType(), True),
            T.StructField("host", T.StringType(), True),
            T.StructField("event_time", T.TimestampType(), True),
        ]
    )

    return spark_fixture.createDataFrame(data, schema=schema)


@pytest.fixture
def input_devices_df(spark_fixture):
    data = [
        {"device_id": 532630305, "browser_type": "Firefox", "os_type": "Ubuntu", "device_type": "Other"},
        {"device_id": 2146219609, "browser_type": "WhatsApp", "os_type": "Other", "device_type": "Spider"},
        {"device_id": 2145574618, "browser_type": "Chrome Mobile", "os_type": "Android", "device_type": "Generic Smartphone"},
        {"device_id": 2143813999, "browser_type": "Mobile Safari UI/WKWebView", "os_type": "iOS", "device_type": "iPhone"},
    ]

    schema = T.StructType(
        [
            T.StructField("device_id", T.IntegerType(), True),
            T.StructField("browser_type", T.StringType(), True),
            T.StructField("os_type", T.StringType(), True),
            T.StructField("device_type", T.StringType(), True),
        ]
    )

    return spark_fixture.createDataFrame(data, schema=schema)


@pytest.mark.unit
def test_should_fail_when_transformation_does_not_match(spark_fixture, input_events_df, input_devices_df, activity_datelist_df):
    input_events_df.createOrReplaceTempView("events")
    input_devices_df.createOrReplaceTempView("devices")

    result_df = transform(spark_fixture, population_query)
    assertDataFrameEqual(result_df, activity_datelist_df)
