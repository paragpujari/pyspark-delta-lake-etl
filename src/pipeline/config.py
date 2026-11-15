from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
SOURCE_DIR = PROJECT_ROOT / "data" / "source"
LAKEHOUSE_DIR = PROJECT_ROOT / "data" / "lakehouse"

PATHS = {
    "orders_csv": SOURCE_DIR / "orders.csv",
    "customers_csv": SOURCE_DIR / "customers.csv",
    "products_csv": SOURCE_DIR / "products.csv",
    "bronze_orders": LAKEHOUSE_DIR / "bronze" / "orders",
    "silver_orders": LAKEHOUSE_DIR / "silver" / "orders",
    "quarantine_orders": LAKEHOUSE_DIR / "quarantine" / "orders",
    "customers_delta": LAKEHOUSE_DIR / "dimensions" / "customers",
    "products_delta": LAKEHOUSE_DIR / "dimensions" / "products",
    "gold_daily_sales": LAKEHOUSE_DIR / "gold" / "daily_sales",
    "gold_category_performance": LAKEHOUSE_DIR / "gold" / "category_performance",
    "gold_customer_ltv": LAKEHOUSE_DIR / "gold" / "customer_lifetime_value",
    "quality_report": LAKEHOUSE_DIR / "quality_report.json",
}


def build_spark(app_name: str = "RetailOrdersLakehouse"):
    """Create Spark with Delta extensions. Requires Java and installed dependencies."""
    from pyspark.sql import SparkSession
    from delta import configure_spark_with_delta_pip

    builder = (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.session.timeZone", "UTC")
    )
    return configure_spark_with_delta_pip(builder).getOrCreate()
