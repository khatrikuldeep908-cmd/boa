from pyspark import pipelines as dp
from pyspark.sql import functions as F

@dp.materialized_view(name="gold_orders_by_date_demo3")
def gold_orders_by_date_demo3():
    return (spark.read.table("orders_silver_demo3")
            .groupBy(F.to_date("order_timestamp").alias("order_date"))
            .agg(F.count("*").alias("total_daily_orders"))
            .select(
                "order_date",
                "total_daily_orders"
            ))