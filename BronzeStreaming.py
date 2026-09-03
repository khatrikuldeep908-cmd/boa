from pyspark import pipelines as dp
from pyspark.sql.functions import current_timestamp, col

@dp.table(name="orders_bronze_demo3")
def orders_bronze_demo1():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .load("/Volumes/boabricks/simbu_new/simbu_lab_data/orders/")
        .select(
            "*",
            current_timestamp().alias("processing_time"),
            col("_metadata.file_name").alias("source_file")
        )
    )