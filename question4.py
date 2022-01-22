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
    .option("header", "true") \
    .option("inferSchema", "true") \
    .schema(schema=schema) \
    .load(commit_file)

print("+----------------------------------------+")
print("+-------------- Question 4 --------------+")
print("+----------------------------------------+")

stFrenchFile = open("dataset/frenchST.txt", "r")
stopWordFrench = stFrenchFile.readlines()
stripedStopWords = set()
for word in stopWordFrench:
    stripedStopWords.add(word.strip())

commit_df.withColumn('word', f.explode(f.split(f.lower(f.col('commit')), ' '))) \
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