from pyspark import pipelines as dp
from pyspark.sql.functions import *

# Gold-level materialized view with an example aggregation
@dp.table
def gold_test_master():
    return (
        spark.read.table("silver_test_table") \
        .groupBy("Category") \
        .agg(countDistinct("book_id").alias("DistinctBooks"))
    )