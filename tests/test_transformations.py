# tests/test_transformations.py
"""
Unit tests for Silver layer transformation logic.
Run with pytest on a Databricks cluster or locally with pyspark.
"""

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
from datetime import datetime


@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder.master("local[*]").appName("tests").getOrCreate()


@pytest.fixture
def sample_orders(spark):
    schema = StructType([
        StructField("order_id", StringType()),
        StructField("customer_id", StringType()),
        StructField("product_id", StringType()),
        StructField("quantity", DoubleType()),
        StructField("unit_price", DoubleType()),
        StructField("order_date", TimestampType()),
        StructField("status", StringType()),
        StructField("region", StringType()),
        StructField("_ingestion_timestamp", TimestampType()),
    ])
    data = [
        ("ORD001", "CUST01", "PROD01", 2.0, 50.0, datetime(2024, 1, 15), "shipped", "uae", datetime(2024, 1, 15, 10, 0)),
        ("ORD002", "CUST02", "PROD02", 1.0, 100.0, datetime(2024, 1, 16), "Cancelled", " ksa ", datetime(2024, 1, 16, 10, 0)),
        ("ORD003", "CUST01", "PROD03", 0.0, 25.0, datetime(2024, 1, 17), "pending", None, datetime(2024, 1, 17, 10, 0)),
        ("ORD001", "CUST01", "PROD01", 3.0, 50.0, datetime(2024, 1, 15), "delivered", "uae", datetime(2024, 1, 18, 10, 0)),  # duplicate
        (None, "CUST04", "PROD04", 1.0, 10.0, datetime(2024, 1, 18), "new", "uae", datetime(2024, 1, 18, 10, 0)),  # null key
    ]
    return spark.createDataFrame(data, schema)


class TestDeduplication:
    def test_removes_duplicates(self, spark, sample_orders):
        from pyspark.sql.functions import col, row_number
        from pyspark.sql.window import Window

        window = Window.partitionBy("order_id").orderBy(col("_ingestion_timestamp").desc())
        df = (
            sample_orders
            .filter(col("order_id").isNotNull())
            .withColumn("_row_num", row_number().over(window))
            .filter(col("_row_num") == 1)
            .drop("_row_num")
        )
        # ORD001 appears twice, should keep only the latest
        assert df.filter(col("order_id") == "ORD001").count() == 1

    def test_keeps_latest_record(self, spark, sample_orders):
        from pyspark.sql.functions import col, row_number
        from pyspark.sql.window import Window

        window = Window.partitionBy("order_id").orderBy(col("_ingestion_timestamp").desc())
        df = (
            sample_orders
            .filter(col("order_id").isNotNull())
            .withColumn("_row_num", row_number().over(window))
            .filter(col("_row_num") == 1)
            .drop("_row_num")
        )
        # Latest ORD001 has quantity=3 and status=delivered
        row = df.filter(col("order_id") == "ORD001").collect()[0]
        assert row["quantity"] == 3.0
        assert row["status"] == "delivered"


class TestCleaning:
    def test_status_standardization(self, spark):
        from pyspark.sql.functions import col, lower, trim, when

        data = [("Cancelled",), ("cancel",), ("shipped",), ("Ship",), ("new",)]
        df = spark.createDataFrame(data, ["status"])

        df_clean = (
            df
            .withColumn("status", lower(trim(col("status"))))
            .withColumn("status",
                when(col("status").isin("cancelled", "canceled", "cancel"), "cancelled")
                .when(col("status").isin("shipped", "ship", "dispatched"), "shipped")
                .when(col("status").isin("pending", "new", "created"), "pending")
                .otherwise(col("status"))
            )
        )
        statuses = [row["status"] for row in df_clean.collect()]
        assert "Cancelled" not in statuses
        assert "cancel" not in statuses
        assert statuses.count("cancelled") == 2
        assert statuses.count("shipped") == 2

    def test_region_standardization(self, spark):
        from pyspark.sql.functions import col, upper, trim, coalesce, lit

        data = [(" uae ",), ("ksa",), (None,)]
        df = spark.createDataFrame(data, ["region"])

        df_clean = (
            df
            .withColumn("region", upper(trim(col("region"))))
            .withColumn("region", coalesce(col("region"), lit("UNKNOWN")))
        )
        regions = [row["region"] for row in df_clean.collect()]
        assert regions == ["UAE", "KSA", "UNKNOWN"]

    def test_null_key_filtering(self, spark, sample_orders):
        from pyspark.sql.functions import col

        df = sample_orders.filter(col("order_id").isNotNull())
        assert df.filter(col("order_id").isNull()).count() == 0

    def test_total_amount_calculation(self, spark):
        from pyspark.sql.functions import col

        data = [(2.0, 50.0), (1.0, 100.0), (0.0, 25.0)]
        df = spark.createDataFrame(data, ["quantity", "unit_price"])
        df = df.withColumn("total_amount", col("quantity") * col("unit_price"))

        amounts = [row["total_amount"] for row in df.collect()]
        assert amounts == [100.0, 100.0, 0.0]
