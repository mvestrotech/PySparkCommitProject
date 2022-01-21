import os
from time import sleep

from pyspark.sql import SparkSession
import pyspark.sql.functions as f

spark = SparkSession \
    .builder \
    .appName("Projet pySpark") \
    .master("local[*]") \
    .getOrCreate()

commit_file = "dataset/full.csv"

commit_df = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load(commit_file)

commit_df.createOrReplaceTempView("commit_table")

question2 = spark.sql(
    """SELECT author, count(commit) As TotalCommit
    FROM commit_table
    WHERE repo = "apache/spark"
    GROUP BY author
    ORDER BY TotalCommit DESC""") \
        .show(n=1, truncate=False)