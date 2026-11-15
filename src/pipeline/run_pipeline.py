import argparse
from datetime import datetime, timezone

from .config import PATHS, SOURCE_DIR, build_spark
from .data_generator import generate_source_data
from .quality import build_quality_report, write_quality_report
from .transformations import ingest_bronze, clean_and_validate, build_gold, write_delta


def run(generate_only: bool = False) -> None:
    generate_source_data(SOURCE_DIR)
    if generate_only:
        print(f"Generated sample CSV files in: {SOURCE_DIR}")
        return

    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")
    batch_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    try:
        bronze = ingest_bronze(spark, str(PATHS["orders_csv"]), str(PATHS["bronze_orders"]), batch_id)

        customers = (spark.read.option("header", True).csv(str(PATHS["customers_csv"]))
            .withColumn("customer_id", __import__("pyspark").sql.functions.upper("customer_id")))
        products = spark.read.option("header", True).csv(str(PATHS["products_csv"]))
        write_delta(customers, str(PATHS["customers_delta"]))
        write_delta(products, str(PATHS["products_delta"]))

        silver, quarantine = clean_and_validate(bronze)
        write_delta(silver, str(PATHS["silver_orders"]))
        write_delta(quarantine, str(PATHS["quarantine_orders"]))

        enriched, daily, category, customer_ltv = build_gold(silver, customers, products)
        write_delta(daily, str(PATHS["gold_daily_sales"]), ["order_year", "order_month"])
        write_delta(category, str(PATHS["gold_category_performance"]))
        write_delta(customer_ltv, str(PATHS["gold_customer_ltv"]))

        report = build_quality_report(bronze, silver, quarantine, enriched)
        write_quality_report(report, PATHS["quality_report"])

        print("\n=== PIPELINE SUMMARY ===")
        for key, value in report.items():
            print(f"{key}: {value}")
        print(f"\nGold daily sales saved to: {PATHS['gold_daily_sales']}")
        print(f"Quality report saved to: {PATHS['quality_report']}")
    finally:
        spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run the Retail Orders Delta Lake ETL")
    parser.add_argument("--generate-only", action="store_true", help="Only generate source CSVs")
    args = parser.parse_args()
    run(generate_only=args.generate_only)
