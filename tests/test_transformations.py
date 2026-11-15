from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import pytest

from src.pipeline.transformations import clean_and_validate


@pytest.fixture(scope="session")
def spark():
    session = (SparkSession.builder.master("local[1]").appName("pipeline-tests")
               .config("spark.ui.enabled", "false").getOrCreate())
    yield session
    session.stop()


def test_invalid_quantity_is_quarantined(spark):
    rows = [
        ("O1", "C1", "P1", "2025-01-01", "2", "10", "0.1", "completed", "south", "file.csv", "2025-01-01 00:00:00", "b1"),
        ("O2", "C1", "P1", "2025-01-01", "0", "10", "0.1", "pending", "south", "file.csv", "2025-01-01 00:00:00", "b1"),
    ]
    columns = ["order_id", "customer_id", "product_id", "order_date", "quantity", "unit_price", "discount", "status", "region", "source_file", "ingestion_timestamp", "batch_id"]
    bronze = spark.createDataFrame(rows, columns).withColumn("ingestion_timestamp", F.to_timestamp("ingestion_timestamp"))
    valid, rejected = clean_and_validate(bronze)
    assert valid.count() == 1
    assert rejected.count() == 1
    assert rejected.first()["rejection_reason"] == "quantity_must_be_positive"


def test_duplicate_order_id_keeps_one_record(spark):
    rows = [
        ("O1", "C1", "P1", "2025-01-01", "1", "10", "0", "completed", "south", "a.csv", "2025-01-01 00:00:00", "b1"),
        ("O1", "C1", "P1", "2025-01-02", "2", "10", "0", "completed", "south", "b.csv", "2025-01-02 00:00:00", "b2"),
    ]
    columns = ["order_id", "customer_id", "product_id", "order_date", "quantity", "unit_price", "discount", "status", "region", "source_file", "ingestion_timestamp", "batch_id"]
    bronze = spark.createDataFrame(rows, columns).withColumn("ingestion_timestamp", F.to_timestamp("ingestion_timestamp"))
    valid, rejected = clean_and_validate(bronze)
    assert valid.count() == 1
    assert valid.first()["quantity"] == 2
    assert rejected.count() == 0
