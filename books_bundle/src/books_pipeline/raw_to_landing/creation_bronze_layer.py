#AUTOLOADER
from pyspark import pipelines as dp
from pyspark.sql.functions import *

# Get pipeline configs
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