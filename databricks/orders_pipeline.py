# Databricks notebook source
# Walkthrough note: upload orders.csv, then enter its Databricks path in the
# widget. This keeps my personal workspace path out of the shared notebook.
dbutils.widgets.text("input_path", "")
input_path = dbutils.widgets.get("input_path")
if not input_path:
    raise ValueError("Set the input_path widget to the uploaded orders.csv path")

from pyspark.sql import Window, functions as F

# Note: I keep bronze, silver, and rejects together in one demo schema.
spark.sql("CREATE SCHEMA IF NOT EXISTS retail_demo")

# Step 1 - Bronze is the raw copy. I keep every CSV value as text so I can
# inspect bad source values later. Overwrite means a rerun starts from this
# file again instead of adding the same eight rows twice.
bronze = spark.read.option("header", True).csv(input_path)
# Spark's CSV reader uses string columns here because schema inference is off.
bronze.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("retail_demo.orders_bronze")

# Step 2 - Rank copies of each order by source_record_id. Rank 1 is the copy
# considered for silver; later copies are rejected. This is a demo rule, not
# a production definition of the latest order (which would need event time).
first_order = Window.partitionBy("order_id").orderBy("source_record_id")
# Note: try_cast converts valid text to typed values and turns bad text into
# NULL. These valid_* columns are helpers for checking and building silver.
checked = (bronze
    .withColumn("order_rank", F.row_number().over(first_order))
    .withColumn("valid_date", F.expr("try_cast(order_date AS DATE)"))
    .withColumn("valid_shipment_date", F.expr("try_cast(shipment_date AS DATE)"))
    .withColumn("valid_amount", F.expr("try_cast(order_amount AS DECIMAL(10,2))"))
    .withColumn("valid_quantity", F.expr("try_cast(quantity AS INT)")))

# Step 3 - Assign one explanation to each bad row. The first matching rule
# wins: a duplicate with a missing customer is labeled as a duplicate.
# No matching rule means error_reason is NULL and the row can enter silver.
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

# Step 4 - Split on error_reason. Rejects keep the original CSV values plus
# the reason, which helps someone correct the source data.
rejects = checked.filter(F.col("error_reason").isNotNull()).select(*bronze.columns, "error_reason")
# Silver gets the accepted rows and typed fields used by downstream reporting.
# The temporary rank and validation columns are left out.
silver = (checked.filter(F.col("error_reason").isNull())
    .select("order_id", "customer_id", F.col("valid_date").alias("order_date"),
            "product_id", F.col("valid_quantity").alias("quantity"),
            F.col("valid_amount").alias("order_amount"), "currency", "order_status", "shipment_date"))

# Step 5 - Check the split before writing tables. Bronze count must equal
# silver plus rejects, and silver must have at most one row per order ID.
assert bronze.count() == silver.count() + rejects.count(), "Rows disappeared during validation"
assert silver.groupBy("order_id").count().filter("count > 1").count() == 0, "Duplicate order in silver"

# Note: output tables are replaced only after both checks pass. A failed
# check leaves the previous silver and rejects tables available to inspect.
rejects.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("retail_demo.orders_rejects")
silver.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("retail_demo.orders_silver")

# Final check for the demo: show layer counts and the rejected rows.
print(f"bronze={bronze.count()}, silver={silver.count()}, rejects={rejects.count()}")
display(spark.table("retail_demo.orders_rejects"))
