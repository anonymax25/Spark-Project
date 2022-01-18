from threading import Thread
import time
start_time = time.time()

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, split, lower, to_timestamp
from datetime import datetime, timedelta


spark = SparkSession \
    .builder \
    .appName("Projet-Q4") \
    .master("local[*]") \
    .getOrCreate()

# For SQL Date Parsing
spark.conf.set("spark.sql.legacy.timeParserPolicy","LEGACY")

commits_file = "data/full.csv"
stopwords_file = "data/englishST.txt"

stopwords_rdd = spark.sparkContext.textFile(stopwords_file)

commits_df = spark.read.format("csv") \
    .option("inferSchema", "true") \
    .option("header", "true") \
    .load(commits_file)


# Question 1
commits_df.groupBy("repo") \
    .count().withColumnRenamed("count","commits") \
    .orderBy(col("commits").desc()) \
    .filter(commits_df.repo.isNotNull()) \
    .limit(10) \
    .show()

# Question 2
commits_df.filter(commits_df.repo == "apache/spark") \
    .groupBy("author") \
    .count().withColumnRenamed("count","commits") \
    .orderBy(col("commits").desc()) \
    .filter(commits_df.author.isNotNull()) \
    .limit(1) \
    .show()

# Question 3
six_months_ago = (datetime.today() - timedelta(days=365//2)).strftime(format='%Y-%m-%d')
two_years_ago = (datetime.today() - timedelta(days=365*2)).strftime(format='%Y-%m-%d')
today = datetime.today().strftime(format='%Y-%m-%d')

commits_df.filter((commits_df.repo == "apache/spark") & (commits_df.author.isNotNull()) & commits_df.date.isNotNull()) \
    .withColumn('parsed_date',to_timestamp(col('date'),'EEE LLL dd kk:mm:ss yyyy Z')) \
    .where(col('parsed_date').between(two_years_ago, today)) \
    .groupBy("author") \
    .count().withColumnRenamed("count","commits") \
    .orderBy(col("commits").desc()) \
    .limit(10) \
    .show()

# Question 4
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