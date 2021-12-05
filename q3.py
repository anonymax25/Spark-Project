from pyspark.sql import SparkSession
from pyspark.sql.types import StringType
from pyspark.sql.window import Window
from pyspark.sql.functions import col, log, month, to_timestamp, udf, when, desc, sum, dense_rank, lit, to_date, from_unixtime, unix_timestamp

from datetime import date, datetime, timedelta
from dateutil.relativedelta import relativedelta

def six_months_ago():
    return date.today() + relativedelta(months=-6)


spark = SparkSession \
    .builder \
    .appName("TP3-bis") \
    .master("local[*]") \
    .getOrCreate()

spark.conf.set("spark.sql.legacy.timeParserPolicy","LEGACY")

commits_file = "data/full.csv"

commits_df = spark.read.format("csv") \
    .option("inferSchema", "true") \
    .option("header", "true") \
    .load(commits_file)

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