import os
from time import sleep

from pyspark.sql import SparkSession
import pyspark.sql.functions as f

spark = SparkSession \
    .builder \
    .appName("Projet pySpark") \
    .master("local[*]") \
    .getOrCreate()

spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

commit_file = "dataset/full.csv"

commit_df = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load(commit_file)

commit_df.createOrReplaceTempView("commit_table")

question3 = spark.sql("""
    SELECT  author as AUTEUR,
            COUNT(commit) AS CONTRIBUTION
    FROM commit_table
    WHERE repo = 'apache/spark'
    AND date IS NOT NULL 
    AND date_format(to_date(date, "EEE LLL dd hh:mm:ss yyyy"), "yyyy-MM-dd") >= add_months(current_date(), -24) 
    group by author
    order by COUNT(commit) DESC
    """) \
    .show(n=1, truncate=False)