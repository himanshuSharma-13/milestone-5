# Databricks notebook source
# Upload data/orders.csv, then set this widget to its Databricks path.
# A widget avoids putting a personal workspace path in the code.
dbutils.widgets.text("input_path", "")
input_path = dbutils.widgets.get("input_path")
if not input_path:
    raise ValueError("Set the input_path widget to the uploaded orders.csv path")

from pyspark.sql import Window, functions as F

spark.sql("CREATE SCHEMA IF NOT EXISTS retail_demo")

# Bronze: retain every source row as text so bad values remain visible.
bronze = spark.read.option("header", True).csv(input_path)
bronze.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("retail_demo.orders_bronze")

# A stable record ID decides which copy of a duplicate order is kept.
# try_cast returns NULL for bad text, which the quality rules reject below.
first_order = Window.partitionBy("order_id").orderBy("source_record_id")
checked = (bronze
    .withColumn("order_rank", F.row_number().over(first_order))
    .withColumn("valid_date", F.expr("try_cast(order_date AS DATE)"))
    .withColumn("valid_shipment_date", F.expr("try_cast(shipment_date AS DATE)"))
    .withColumn("valid_amount", F.expr("try_cast(order_amount AS DECIMAL(10,2))"))
    .withColumn("valid_quantity", F.expr("try_cast(quantity AS INT)")))

# One simple reason per rejected row makes the exception table easy to read.
# The order of these rules decides which reason is shown if several fail.
checked = checked.withColumn("error_reason",
    F.when(F.col("order_id").isNull() | (F.trim("order_id") == ""), "MISSING_ORDER_ID")
     .when(F.col("order_rank") > 1, "DUPLICATE_ORDER_ID")
     .when(F.col("customer_id").isNull() | (F.trim("customer_id") == ""), "MISSING_CUSTOMER_ID")
     .when(F.col("product_id").isNull() | (F.trim("product_id") == ""), "MISSING_PRODUCT_ID")
     .when(F.col("valid_date").isNull() | (F.col("valid_date") > F.current_date()), "INVALID_ORDER_DATE")
     .when(F.col("valid_quantity").isNull() | (F.col("valid_quantity") <= 0), "INVALID_QUANTITY")
     .when(F.col("valid_amount").isNull() | (F.col("valid_amount") < 0), "INVALID_AMOUNT")
     .when(F.col("currency").isNull() | ~F.col("currency").isin("INR", "USD"), "INVALID_CURRENCY")
     .when(F.col("order_status").isNull() | ~F.col("order_status").isin("PLACED", "SHIPPED", "CANCELLED"), "INVALID_STATUS")
     .when((F.col("order_status") == "SHIPPED") & F.col("valid_shipment_date").isNull(), "MISSING_SHIPMENT_DATE")
     .when(F.col("shipment_date").isNotNull() & F.col("valid_shipment_date").isNull(), "INVALID_SHIPMENT_DATE")
     .when(F.col("valid_shipment_date") < F.col("valid_date"), "INVALID_SHIPMENT_DATE"))

# Split the checked rows. Rejects are retained for correction and reruns.
rejects = checked.filter(F.col("error_reason").isNotNull()).select(*bronze.columns, "error_reason")
silver = (checked.filter(F.col("error_reason").isNull())
    .select("order_id", "customer_id", F.col("valid_date").alias("order_date"),
            "product_id", F.col("valid_quantity").alias("quantity"),
            F.col("valid_amount").alias("order_amount"), "currency", "order_status", "shipment_date"))

# These two assertions catch silent row loss and duplicate trusted orders.
assert bronze.count() == silver.count() + rejects.count(), "Rows disappeared during validation"
assert silver.groupBy("order_id").count().filter("count > 1").count() == 0, "Duplicate order in silver"

# Overwrite makes a rerun of this same small file give the same result.
rejects.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("retail_demo.orders_rejects")
silver.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("retail_demo.orders_silver")

print(f"bronze={bronze.count()}, silver={silver.count()}, rejects={rejects.count()}")
display(spark.table("retail_demo.orders_rejects"))
