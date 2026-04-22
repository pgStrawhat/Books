# Pipeline source code
# Imports
# When using python, we import the pipelines and the functions modules
from pyspark import pipelines as dp
from pyspark.sql.functions import *

# Get pipeline configs
# We set these configs when we created the pipeline
# The code below captures these configs as python variables
catalog_name = spark.conf.get("catalog_name")
schema_name = spark.conf.get("schema_name")
dataset_name = spark.conf.get("dataset_name")

# Define the source path using the configs
source_path = f'/Volumes/{catalog_name}/{schema_name}/{dataset_name}/books-csv/'

# Bronze table
# Read the CSV data from the source location using Auto Loader into a bronze-level table
@dp.table
def bronze_test_table():
    return (
        spark.readStream.format("cloudFiles") \
        .option("cloudFiles.format", "csv") \
        .option("header", "true") \
        .option("delimiter", ";") \
        .option("cloudFiles.inferColumnTypes", "true") \
        .load(source_path)
    )
    
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