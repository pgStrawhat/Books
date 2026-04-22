# Databricks notebook source
data_catalog = spark.sql("SELECT current_catalog()").collect()[0][0]
dataset_volume_name = "bookstore_dataset"
dataset_bookstore = f"/Volumes/{data_catalog}/default/{dataset_volume_name}"

# COMMAND ----------

def load_csv_file(raw_books_dir, raw_new_books_dir, new_file):
    print(f"Loading {new_file} books file to the bookstore dataset")
    dbutils.fs.cp(f"{raw_new_books_dir}/{new_file}", f"{raw_books_dir}/{new_file}")


def load_new_csv_data():
    books_present = []
    raw_books_dir = f"{dataset_bookstore}/books-csv"
    raw_new_books_dir = f"{dataset_bookstore}/books-csv-new"
    old_books = dbutils.fs.ls(raw_books_dir)
    books_present = [book.name for book in old_books]
    for new_book in dbutils.fs.ls(raw_new_books_dir):
        new_file = new_book.name
        if new_file not in books_present:
            load_csv_file(raw_books_dir, raw_new_books_dir, new_file)
