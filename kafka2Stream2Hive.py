from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType, DoubleType


spark = SparkSession.builder \
    .appName("KafkaToHive") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.11:2.3.0") \
    .config("spark.sql.warehouse.dir", "/apps/hive/warehouse") \
    .config("hive.metastore.uris", "thrift://sandbox-hdp.hortonworks.com:9083") \
    .enableHiveSupport() \
    .getOrCreate()

df = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "sandbox-hdp.hortonworks.com:6667") \
  .option("subscribe", "reddit_posts_sentiments") \
  .option("startingOffsets", "earliest") \
  .load()

schema = StructType() \
    .add("id", StringType()) \
    .add("subredditvar", StringType()) \
    .add("title", StringType()) \
    .add("selftext", StringType()) \
    .add("compound_score", DoubleType()) \
    .add("sentiment", StringType())

df = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

spark.sql("USE reddit_sentiments")

query = df.writeStream \
    .outputMode("append") \
    .format("parquet") \
    .option("path", "/apps/hive/warehouse/reddit_sentiments.db/reddit_sentiment_analysis") \
    .option("checkpointLocation", "/home/maria_dev/Spark/spark_logs") \
    .start("reddit_sentiments.reddit_sentiment_analysis") \
    .awaitTermination()
