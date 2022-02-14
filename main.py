from time import sleep

from pyspark.sql import SparkSession
import pyspark.sql.functions as f
from pyspark.sql.types import StructType, StringType

spark = SparkSession \
    .builder \
    .appName("Projet pySpark") \
    .config("spark.executor.memory", "2g") \
    .config("spark.executor.cores", "2") \
    .master("local[*]") \
    .getOrCreate()

commit_file = "dataset/full.csv"

schema = StructType() \
    .add("commit", StringType(), True) \
    .add("author", StringType(), True) \
    .add("date", StringType(), True) \
    .add("message", StringType(), True) \
    .add("repo", StringType(), True)

commit_df = spark.read.format("csv") \
    .option("wholeFile", True) \
    .option("multiline", True) \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .schema(schema=schema) \
    .load(commit_file) \
    .repartition(10)

print("+----------------------------------------+")
print("+-------------- Question 1 --------------+")
print("+----------------------------------------+")

commit_df.groupby("repo") \
    .agg(f.count("commit").alias("Total-Commit")) \
    .filter("repo is not NULL") \
    .sort(f.desc("Total-Commit")) \
    .show(n=10, truncate=False)

print("+----------------------------------------+")
print("+-------------- Question 2 --------------+")
print("+----------------------------------------+")

commit_df.groupBy("author", "repo") \
    .agg(f.count("commit").alias("Total-Commit")) \
    .sort(f.desc("Total-Commit")) \
    .filter("repo  = 'apache/spark'") \
    .limit(1) \
    .show(truncate=False)

print("+----------------------------------------+")
print("+-------------- Question 3 --------------+")
print("+----------------------------------------+")

twentyFourMonth = f.add_months(f.current_timestamp(), -24).cast("timestamp").cast("long")

commit_df.select("author", "repo", "date") \
    .filter("repo = 'apache/spark'") \
    .withColumn("dateFormat", f.col("date").substr(5, 20)) \
    .withColumn("dateTrim", f.when(f.unix_timestamp(f.col("dateFormat"), "MMM d HH:mm:ss yyyy ")
                                   .isNotNull(), f.unix_timestamp(f.col("dateFormat"), "MMM d HH:mm:ss yyyy "))
                .when(f.unix_timestamp(f.col("dateFormat"), "MMM dd HH:mm:ss yyyy")
                      .isNotNull(), f.unix_timestamp(f.col("dateFormat"), "MMM dd HH:mm:ss yyyy"))
                .otherwise(None)
                ) \
    .where(f.col("dateTrim").isNotNull()) \
    .where((f.col("dateTrim") >= twentyFourMonth)) \
    .groupby("author") \
    .agg(f.count("repo").alias("Total-Commit")) \
    .sort(f.desc("Total-Commit")) \
    .show(n=10, truncate=False)

print("+----------------------------------------+")
print("+-------------- Question 4 --------------+")
print("+----------------------------------------+")

stFrenchFile = open("dataset/englishST.txt", "r")
stopWordFrench = stFrenchFile.readlines()
stripedStopWords = set()
for word in stopWordFrench:
    stripedStopWords.add(word.strip())

commit_df.withColumn('word', f.explode(f.split(f.lower(f.col('commit')), ' '))) \
    .filter(f.col('word').isin(stripedStopWords) == False) \
    .filter(f.length(f.col('word')) > 2) \
    .groupBy('word') \
    .agg(f.count("word").alias("Total-Word")) \
    .sort(f.desc("Total-Word")) \
    .show(n=10)

sleep(1000)
