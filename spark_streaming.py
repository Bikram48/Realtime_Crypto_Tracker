from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType
from dotenv import load_dotenv
import os

load_dotenv()

AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY")
AWS_SECRET_KEY = os.getenv("AWS_SECRET_KEY")


spark = SparkSession.builder \
    .appName("KafkaCryptoPricesStream") \
    .config("spark.jars.packages", 
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0,"
            "org.apache.hadoop:hadoop-aws:3.3.1") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", 
            "com.amazonaws.auth.profile.ProfileCredentialsProvider") \
    .getOrCreate()




KAFKA_BROKER = "localhost:9092"
KAFKA_TOPIC = "crypto_prices"
S3_BUCKET = "s3a://cyptopricebucket/data/"

# Define schema for incoming JSON data
schema = StructType([
    StructField("rank", StringType(), True),
    StructField("coin", StringType(), True),
    StructField("price", StringType(), True),
    StructField("1h", StringType(), True),
    StructField("24h", StringType(), True),
    StructField("7d", StringType(), True),
    StructField("24h Volume", StringType(), True),
    StructField("market cap", StringType(), True)
])

# Read data stream from Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BROKER) \
    .option("subscribe", KAFKA_TOPIC) \
    .option("startingOffsets", "earliest") \
    .load()

parsed_df = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

# Write the parsed stream data to S3 in Parquet format
query = parsed_df.writeStream \
    .format("parquet") \
    .option("checkpointLocation", "/tmp/kafka_checkpoint") \
    .option("path", S3_BUCKET) \
    .trigger(processingTime="30 seconds") \
    .start()

query.awaitTermination()


