import time
start_time = time.time()

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, split, lower


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

commits_df.where(col("message").isNotNull()) \
    .select(explode(split("message", " ")).alias("words")) \
    .select(lower(col('words')).alias('words')) \
    .where(col("words").isin(stopwords_rdd.collect()) == False) \
    .groupBy("words") \
    .count() \
    .orderBy(col("count").desc()) \
    .limit(10) \
    .show()

print("--- " + str((time.time() - start_time)) +  " secondes ---")