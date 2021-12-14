import time
start_time = time.time()

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession \
    .builder \
    .appName("Projet-Q1") \
    .master("local[*]") \
    .getOrCreate()

commits_file = "data/full.csv"

commits_df = spark.read.format("csv") \
    .option("inferSchema", "true") \
    .option("header", "true") \
    .load(commits_file)


commits_df.groupBy("repo") \
    .count().withColumnRenamed("count","commits") \
    .orderBy(col("commits").desc()) \
    .filter(commits_df.repo.isNotNull()) \
    .limit(10) \
    .show()

print("--- " + str((time.time() - start_time)) +  " secondes ---")