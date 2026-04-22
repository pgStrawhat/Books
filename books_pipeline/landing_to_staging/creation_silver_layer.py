from pyspark import pipelines as dp
from pyspark.sql.functions import *

# Silver-level table with some example transformation logic and data quality expectations
@dp.table
# The first expectation will warn us if the 'Country' column contains any values other than USA, India, and Pakistan
# The second expectation will drop any records where the 'Role' column is null or not one of the expected values
# The third expectation will fail the pipeline if the 'ID' column is null
@dp.expect_or_fail("id_not_null", "book_id IS NOT NULL")
def silver_test_table():
    return (
        spark.readStream.table("bronze_test_table") \
        .withColumn("price", col("price").cast("int")) \
        .withColumn("Category", upper(col("Category"))) \
        .select("book_id", "title", "author", "category", "price")
    )

# Gold-level materialized view with an example aggregation
@dp.table
def gold_test_master():
    return (
        spark.read.table("silver_test_table") \
        .groupBy("Category") \
        .agg(countDistinct("book_id").alias("DistinctBooks"))
    )