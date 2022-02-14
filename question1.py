import os
from time import sleep

from pyspark.sql import SparkSession
import pyspark.sql.functions as f
from pyspark.sql.types import StructType, StringType

spark = SparkSession \
    .builder \
    .appName("Projet pySpark") \
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
    .load(commit_file)

print("+----------------------------------------+")
print("+-------------- Question 1 --------------+")
print("+----------------------------------------+")

commit_df.groupby("repo") \
    .agg(f.count("commit").alias("Total-Commit")) \
    .filter("repo is not NULL") \
    .sort(f.desc("Total-Commit")) \
    .show(n=10, truncate=False)

# +---------------------+------------+
# |repo                 |Total-Commit|
# +---------------------+------------+
# |chromium/chromium    |895846      |
# |torvalds/linux       |816748      |
# |llvm/llvm-project    |355433      |
# |freebsd/freebsd-src  |242564      |
# |openbsd/src          |203484      |
# |gcc-mirror/gcc       |174277      |
# |rust-lang/rust       |136070      |
# |apple/swift          |113355      |
# |tensorflow/tensorflow|105709      |
# |python/cpython       |105058      |
# +---------------------+------------+