from pyspark.sql import SparkSession
from pyspark.sql.types import StringType
from pyspark.sql.window import Window
from pyspark.sql.functions import col, udf, when, desc, sum, dense_rank, lit

spark = SparkSession \
    .builder \
    .appName("TP3-bis") \
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