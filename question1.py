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

question1 = spark.sql(
    """SELECT repo, count(commit) As TotalCommit
    FROM commit_table
    WHERE repo IS NOT NULL
    GROUP BY repo
    ORDER BY TotalCommit DESC""") \
        .show(n=10, truncate=False)