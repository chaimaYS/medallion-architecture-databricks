# Databricks notebook source
# MAGIC %md
# MAGIC # Gold Layer — Business-Ready Aggregations
# MAGIC
# MAGIC Builds dimensional models and aggregated views for:
# MAGIC - BI dashboards (Power BI, Tableau)
# MAGIC - ML feature stores
# MAGIC - Executive reporting
# MAGIC - API consumption

# COMMAND ----------

from pyspark.sql.functions import (
    col, sum, count, avg, min, max, countDistinct,
    current_timestamp, datediff, current_date,
    round as spark_round, when, months_between
)

# COMMAND ----------

CATALOG = "main"
SILVER_SCHEMA = "silver"
GOLD_SCHEMA = "gold"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold Table 1: Daily Sales Summary (Fact Table)

# COMMAND ----------

def build_daily_sales_summary():
    """
    Aggregated daily sales metrics by region.
    Serves executive dashboards and trend analysis.
    """
    df_orders = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.orders")
    
    df_daily = (
        df_orders
        .filter(col("status") != "cancelled")
        .groupBy("order_date", "region")
        .agg(
            count("order_id").alias("total_orders"),
            countDistinct("customer_id").alias("unique_customers"),
            spark_round(sum("total_amount"), 2).alias("total_revenue"),
            spark_round(avg("total_amount"), 2).alias("avg_order_value"),
            spark_round(avg("quantity"), 2).alias("avg_quantity"),
            sum(when(col("status") == "delivered", 1).otherwise(0)).alias("delivered_orders"),
            sum(when(col("status") == "pending", 1).otherwise(0)).alias("pending_orders"),
        )
        .withColumn("delivery_rate", 
            spark_round(col("delivered_orders") / col("total_orders") * 100, 2)
        )
        .withColumn("_gold_processed_at", current_timestamp())
        .orderBy("order_date", "region")
    )
    
    (
        df_daily.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(f"{CATALOG}.{GOLD_SCHEMA}.daily_sales_summary")
    )
    
    print(f"Gold: daily_sales_summary — {df_daily.count()} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold Table 2: Customer 360 (Dimension Table)

# COMMAND ----------

def build_customer_360():
    """
    Customer-level aggregated profile for segmentation, 
    churn prediction, and personalization.
    """
    df_orders = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.orders")
    
    df_customer = (
        df_orders
        .groupBy("customer_id")
        .agg(
            count("order_id").alias("total_orders"),
            countDistinct("product_id").alias("unique_products"),
            spark_round(sum("total_amount"), 2).alias("lifetime_value"),
            spark_round(avg("total_amount"), 2).alias("avg_order_value"),
            min("order_date").alias("first_order_date"),
            max("order_date").alias("last_order_date"),
            sum(when(col("status") == "cancelled", 1).otherwise(0)).alias("cancelled_orders"),
            sum(when(col("status") == "delivered", 1).otherwise(0)).alias("delivered_orders"),
        )
        .withColumn("days_since_last_order",
            datediff(current_date(), col("last_order_date"))
        )
        .withColumn("customer_tenure_months",
            spark_round(months_between(current_date(), col("first_order_date")), 0)
        )
        .withColumn("cancellation_rate",
            spark_round(col("cancelled_orders") / col("total_orders") * 100, 2)
        )
        # Customer segmentation based on RFM-like logic
        .withColumn("customer_segment",
            when(col("lifetime_value") > 10000, "VIP")
            .when((col("lifetime_value") > 5000) & (col("days_since_last_order") < 90), "HIGH_VALUE")
            .when(col("days_since_last_order") > 180, "AT_RISK")
            .when(col("total_orders") == 1, "NEW")
            .otherwise("REGULAR")
        )
        .withColumn("_gold_processed_at", current_timestamp())
    )
    
    (
        df_customer.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(f"{CATALOG}.{GOLD_SCHEMA}.customer_360")
    )
    
    print(f"Gold: customer_360 — {df_customer.count()} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold Table 3: Product Performance

# COMMAND ----------

def build_product_performance():
    """
    Product-level metrics for inventory planning and pricing optimization.
    """
    df_orders = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.orders")
    
    df_product = (
        df_orders
        .filter(col("status") != "cancelled")
        .groupBy("product_id")
        .agg(
            count("order_id").alias("total_orders"),
            countDistinct("customer_id").alias("unique_buyers"),
            spark_round(sum("quantity"), 2).alias("total_units_sold"),
            spark_round(sum("total_amount"), 2).alias("total_revenue"),
            spark_round(avg("unit_price"), 2).alias("avg_selling_price"),
            countDistinct("region").alias("regions_sold_in"),
        )
        .withColumn("revenue_per_buyer",
            spark_round(col("total_revenue") / col("unique_buyers"), 2)
        )
        .withColumn("_gold_processed_at", current_timestamp())
        .orderBy(col("total_revenue").desc())
    )
    
    (
        df_product.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(f"{CATALOG}.{GOLD_SCHEMA}.product_performance")
    )
    
    print(f"Gold: product_performance — {df_product.count()} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Execute Gold Pipeline

# COMMAND ----------

build_daily_sales_summary()
build_customer_360()
build_product_performance()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Post-Load: Optimize & Analyze

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE main.gold.daily_sales_summary ZORDER BY (order_date);
# MAGIC OPTIMIZE main.gold.customer_360 ZORDER BY (customer_id);
# MAGIC OPTIMIZE main.gold.product_performance ZORDER BY (product_id);

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Quick validation
# MAGIC SELECT 'daily_sales_summary' AS table_name, COUNT(*) AS row_count FROM main.gold.daily_sales_summary
# MAGIC UNION ALL
# MAGIC SELECT 'customer_360', COUNT(*) FROM main.gold.customer_360
# MAGIC UNION ALL
# MAGIC SELECT 'product_performance', COUNT(*) FROM main.gold.product_performance;
