from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession \
    .builder \
    .appName("Projet-Q2") \
    .master("local[*]") \
    .getOrCreate()

commits_file = "data/full.csv"

commits_df = spark.read.format("csv") \
    .option("inferSchema", "true") \
    .option("header", "true") \
    .load(commits_file)

commits_df.filter(commits_df.repo == "apache/spark") \
    .groupBy("author") \
    .count().withColumnRenamed("count","commits") \
    .orderBy(col("commits").desc()) \
    .filter(commits_df.author.isNotNull()) \
    .limit(1) \
    .show()