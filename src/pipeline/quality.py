import json
from pathlib import Path
from pyspark.sql import functions as F


def build_quality_report(bronze, silver, quarantine, enriched) -> dict:
    bronze_count = bronze.count()
    silver_count = silver.count()
    quarantine_count = quarantine.count()
    duplicate_keys = (silver.groupBy("order_id").count().filter(F.col("count") > 1).count())
    missing_customer_dim = enriched.filter(F.col("customer_name").isNull()).count()
    missing_product_dim = enriched.filter(F.col("product_name").isNull()).count()
    return {
        "bronze_records": bronze_count,
        "silver_records": silver_count,
        "quarantined_records": quarantine_count,
        "silver_duplicate_order_ids": duplicate_keys,
        "orders_without_customer_match": missing_customer_dim,
        "orders_without_product_match": missing_product_dim,
        "checks": {
            "silver_order_id_unique": duplicate_keys == 0,
            "all_silver_records_have_order_id": silver.filter(F.col("order_id").isNull() | (F.col("order_id") == "")).limit(1).count() == 0,
            "all_silver_quantities_positive": silver.filter(F.col("quantity") <= 0).limit(1).count() == 0,
        },
    }


def write_quality_report(report: dict, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(report, indent=2), encoding="utf-8")
