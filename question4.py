import os
from time import sleep

from pyspark.sql import SparkSession
import pyspark.sql.functions as f

spark = SparkSession \
    .builder \
    .appName("Projet pySpark") \
    .master("local[*]") \
    .config("spark.executor.memory", "10g") \
    .config("spark.driver.memory", "10g") \
    .config("spark.sql.shuffle.partitions", "7") \
    .config("spark.executor.instances", "4") \
    .config("spark.executor.cores", "26") \
    .getOrCreate()

spark.sparkContext._conf.getAll() 


commit_file = "dataset/full.csv"

commit_df = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load(commit_file)

commit_df.createOrReplaceTempView("commit_table")

stFrenchFile = open("dataset/frenchST.txt", "r")
stopWordFrench = stFrenchFile.readlines()
stripedStopWords = set()
for word in stopWordFrench:
    stripedStopWords.add(word.strip())

question4 = commit_df.withColumn('word', f.explode(f.split(f.lower(f.col('commit')), ' '))) \
    .filter(f.col('word').isin(stripedStopWords) == False) \
    .filter(f.length(f.col('word')) > 2) \
    .groupBy('word') \
    .count().sort('count', ascending=False) \
    .show(n=10)

# +-------------------+-------+                                                   
# |               word|  count|
# +-------------------+-------+
# |                the|5492988|
# |     signed-off-by:|1867247|
# |               this|1654049|
# |                for|1564985|
# |                and|1439672|
# |       reviewed-by:| 938526|
# |               that| 908154|
# |               with| 681533|
# |cr-commit-position:| 669387|
# |               from| 639368|
# +-------------------+-------+