import time
start_time = time.time()

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, udf, explode, split, lit
from pyspark.sql.types import StringType


spark = SparkSession \
    .builder \
    .appName("Projet-Q4") \
    .master("local[*]") \
    .getOrCreate()

commits_file = "data/full.csv"
stopwords_file = "data/englishST.txt"

stopwords_rdd = spark.sparkContext.textFile(stopwords_file)

commits_df = spark.read.format("csv") \
    .option("inferSchema", "true") \
    .option("header", "true") \
    .load(commits_file)

def str_func(word):
    return "-" + str(word) + "-"

str_col = udf(str_func, StringType())


commits_df.where(col("message").isNotNull()) \
    .select(explode(split("message", " ")).alias("words")) \
    .where(col("words").isin(stopwords_rdd.collect()) == False) \
    .groupBy("words") \
    .count() \
    .orderBy(col("count").desc()) \
    .limit(10) \
    .show()

print("--- " + str((time.time() - start_time)) +  " secondes ---")