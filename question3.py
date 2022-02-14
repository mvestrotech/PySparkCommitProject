from pyspark.sql.functions import col, when, add_months, current_timestamp, unix_timestamp, desc

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
print("+-------------- Question 3 --------------+")
print("+----------------------------------------+")

twentyFourMonth = add_months(current_timestamp(), -24).cast("timestamp").cast("long")

commit_df.select("author", "repo", "date") \
    .filter("repo = 'apache/spark'") \
    .withColumn("dateFormat", col("date").substr(5, 20)) \
    .withColumn("dateTrim", when(unix_timestamp(col("dateFormat"), "MMM d HH:mm:ss yyyy ")
                                 .isNotNull(), unix_timestamp(col("dateFormat"), "MMM d HH:mm:ss yyyy "))
                .when(unix_timestamp(col("dateFormat"), "MMM dd HH:mm:ss yyyy")
                      .isNotNull(), unix_timestamp(col("dateFormat"), "MMM dd HH:mm:ss yyyy"))
                .otherwise(None)
                ) \
    .where(col("dateTrim").isNotNull()) \
    .where((col("dateTrim") >= twentyFourMonth)) \
    .groupby("author") \
    .agg(f.count("repo").alias("Total-Commit")) \
    .sort(desc("Total-Commit")) \
    .show(n=10, truncate=False)

# +----------------------------------------+------------+
# |author                                  |Total-Commit|
# +----------------------------------------+------------+
# |HyukjinKwon <gurwls223@apache.org>      |136         |
# |Wenchen Fan <wenchen@databricks.com>    |126         |
# |Max Gekk <max.gekk@gmail.com>           |105         |
# |Kent Yao <yaooqinn@hotmail.com>         |97          |
# |Dongjoon Hyun <dongjoon@apache.org>     |86          |
# |Liang-Chi Hsieh <viirya@gmail.com>      |86          |
# |Dongjoon Hyun <dhyun@apple.com>         |85          |
# |yi.wu <yi.wu@databricks.com>            |80          |
# |Kousuke Saruta <sarutak@oss.nttdata.com>|62          |
# |Yuming Wang <yumwang@ebay.com>          |59          |
# +----------------------------------------+------------+