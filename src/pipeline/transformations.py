from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType


ORDER_COLUMNS = ["order_id", "customer_id", "product_id", "order_date", "quantity", "unit_price", "discount", "status", "region"]


def ingest_bronze(spark, csv_path: str, delta_path: str, batch_id: str) -> DataFrame:
    """Preserve source fields as strings and attach ingestion lineage."""
    raw = (spark.read.option("header", True).option("inferSchema", False)
           .option("mode", "PERMISSIVE").csv(csv_path))
    return (raw.withColumn("source_file", F.input_file_name())
            .withColumn("ingestion_timestamp", F.current_timestamp())
            .withColumn("batch_id", F.lit(batch_id))
            .write.format("delta").mode("overwrite").save(delta_path)
            or spark.read.format("delta").load(delta_path))


def clean_and_validate(bronze: DataFrame):
    """Return (valid_deduplicated_orders, rejected_rows)."""
    typed = (bronze
        .withColumn("order_id", F.trim(F.col("order_id")))
        .withColumn("customer_id", F.upper(F.trim(F.col("customer_id"))))
        .withColumn("product_id", F.upper(F.trim(F.col("product_id"))))
        .withColumn("order_date", F.to_date(F.trim(F.col("order_date")), "yyyy-MM-dd"))
        .withColumn("quantity", F.col("quantity").cast(IntegerType()))
        .withColumn("unit_price", F.col("unit_price").cast(DoubleType()))
        .withColumn("discount", F.col("discount").cast(DoubleType()))
        .withColumn("status", F.initcap(F.lower(F.trim(F.col("status")))))
        .withColumn("region", F.initcap(F.lower(F.trim(F.col("region")))))
    )

    reason = F.concat_ws("; ",
        F.when(F.col("order_id").isNull() | (F.col("order_id") == ""), "missing_order_id"),
        F.when(F.col("customer_id").isNull() | (F.col("customer_id") == ""), "missing_customer_id"),
        F.when(F.col("product_id").isNull() | (F.col("product_id") == ""), "missing_product_id"),
        F.when(F.col("order_date").isNull(), "invalid_or_missing_order_date"),
        F.when(F.col("quantity").isNull() | (F.col("quantity") <= 0), "quantity_must_be_positive"),
        F.when(F.col("unit_price").isNull() | (F.col("unit_price") < 0), "unit_price_must_be_non_negative"),
        F.when(F.col("discount").isNull() | (F.col("discount") < 0) | (F.col("discount") > 1), "discount_must_be_between_0_and_1"),
    )
    checked = typed.withColumn("rejection_reason", reason)
    rejected = checked.filter(F.col("rejection_reason") != "")
    valid = checked.filter(F.col("rejection_reason") == "").drop("rejection_reason")

    window = Window.partitionBy("order_id").orderBy(F.col("ingestion_timestamp").desc(), F.col("batch_id").desc())
    valid = (valid.withColumn("_row_num", F.row_number().over(window))
             .filter(F.col("_row_num") == 1).drop("_row_num")
             .withColumn("gross_amount", F.round(F.col("quantity") * F.col("unit_price"), 2))
             .withColumn("discount_amount", F.round(F.col("quantity") * F.col("unit_price") * F.col("discount"), 2))
             .withColumn("net_amount", F.round(F.col("quantity") * F.col("unit_price") * (1 - F.col("discount")), 2)))
    return valid, rejected


def build_gold(silver: DataFrame, customers: DataFrame, products: DataFrame):
    """Enrich Silver orders and create three business-facing Gold aggregates."""
    enriched = (silver
        .join(F.broadcast(customers), on="customer_id", how="left")
        .join(F.broadcast(products), on="product_id", how="left"))

    daily = (enriched
        .withColumn("order_year", F.year("order_date"))
        .withColumn("order_month", F.month("order_date"))
        .groupBy("order_year", "order_month", "order_date", "region")
        .agg(F.countDistinct("order_id").alias("total_orders"),
             F.sum("quantity").alias("units_sold"),
             F.round(F.sum("net_amount"), 2).alias("net_sales")))

    category = (enriched.groupBy("category", "region")
        .agg(F.countDistinct("order_id").alias("total_orders"),
             F.sum("quantity").alias("units_sold"),
             F.round(F.sum("net_amount"), 2).alias("net_sales"))
        .orderBy(F.col("net_sales").desc()))

    customer_ltv = (enriched.groupBy("customer_id", "customer_name", "segment", "city")
        .agg(F.countDistinct("order_id").alias("lifetime_orders"),
             F.round(F.sum("net_amount"), 2).alias("lifetime_value"),
             F.max("order_date").alias("last_order_date"))
        .orderBy(F.col("lifetime_value").desc()))
    return enriched, daily, category, customer_ltv


def write_delta(df: DataFrame, path: str, partition_by=None) -> None:
    writer = df.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
    if partition_by:
        writer = writer.partitionBy(*partition_by)
    writer.save(path)
