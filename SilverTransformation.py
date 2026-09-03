from pyspark import pipelines as dp
from pyspark.sql import functions as F

@dp.temporary_view()
def orders_silver_prep():
    return (spark.readStream.table("orders_bronze_demo3")
            .select(
                "order_id",
                F.to_timestamp("order_timestamp").alias("order_timestamp"),
                "customer_id",
                "notifications"
            ))

@dp.table(name="orders_silver_demo3")
@dp.expect("order_timestamp_not_future", "order_timestamp <= current_timestamp()")
@dp.expect_or_drop("notification_valid", "notifications IN ('Y', 'N')")
def orders_silver_demo2():
    return spark.readStream.table("orders_silver_prep")

@dp.table(name="orders_bronze_dirtydata")
def orders_bronze_dirtydata():
    return (spark.readStream.table("orders_silver_prep")
            .filter(
                (F.col("order_timestamp") > F.current_timestamp()) |
                F.col("order_timestamp").isNull() |
                (~F.col("notifications").isin("Y", "N")) |
                F.col("notifications").isNull()
            ))