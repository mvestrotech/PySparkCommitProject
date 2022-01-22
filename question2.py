import pyspark.sql.functions as f
from pyspark.sql import SparkSession
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
print("+-------------- Question 2 --------------+")
print("+----------------------------------------+")

commit_df.groupBy("author", "repo")\
    .agg(f.count("commit").alias("Total-Commit"))\
    .sort(f.desc("Total-Commit"))\
    .filter("repo  = 'apache/spark'")\
    .limit(1)\
    .show(truncate=False)

# +---------------------------------------+------------+------------+
# |author                                 |repo        |Total-Commit|
# +---------------------------------------+------------+------------+
# |Matei Zaharia <matei@eecs.berkeley.edu>|apache/spark|1316        |
# +---------------------------------------+------------+------------+