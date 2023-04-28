import sqlite3
import urllib.request
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_timestamp, window
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

# Define Kafka topic names
topic_names = ['ethereum', 'dogecoin', 'tether', 'bitcoin', 'idex','goldmaxcoin']

# Define schema for incoming JSON data
json_schema = StructType([
    StructField("price", DoubleType()),
    StructField("volume", DoubleType()),
    StructField("base", StringType()),
    StructField("quote", StringType()),
    StructField("timestamp", DoubleType())
])

# Initialize Spark session
spark = SparkSession.builder.appName("CryptoDataProcessing").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# Download SQLite JDBC driver
driver_url = "https://repo1.maven.org/maven2/org/xerial/sqlite-jdbc/3.36.0.1/sqlite-jdbc-3.36.0.1.jar"
print(os.path.basename(driver_url))
driver_filename = os.path.basename(driver_url)
temp_dir = "/tmp"
driver_path = os.path.join(temp_dir, driver_filename)
urllib.request.urlretrieve(driver_url, driver_path)

# Read data from Kafka
kafka_df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", ",".join(topic_names)) \
    .option("startingOffsets", "latest") \
    .load()

# Parse JSON data and extract relevant fields
parsed_df = kafka_df \
    .select(from_json(col("value").cast("string"), json_schema).alias("data")) \
    .selectExpr("data.base","data.quote","data.price", "data.volume",  "data.timestamp")

# Convert timestamp to Spark timestamp format
processed_df = parsed_df \
    .withColumn("timestamp", to_timestamp(col("timestamp") / 1000))

# Aggregate data by base and window of 60 seconds
agg_df = processed_df \
    .groupBy(col("base"), window(col("timestamp"), "60 seconds")) \
    .agg({"price": "mean", "volume": "sum"}) \
    .withColumnRenamed("avg(price)", "price") \
    .withColumnRenamed("sum(volume)", "volume")
    



# Write processed data to separate tables in sqlite3 database
def write_to_sqlite(df, epoch_id):
    conn = sqlite3.connect("w60s.db")
    for topic in topic_names:
        topic_df = df.filter(col("base") == topic)
        table_name = topic + "_60s"
        topic_df.write \
            .format("jdbc") \
            .option("url", "jdbc:sqlite:w60s.db") \
            .option("dbtable", table_name) \
            .option("mode", "append") \
            .option("driver", "org.sqlite.JDBC") \
            .mode("append")\
            .save()
    conn.commit()
    conn.close()


# Start the query and wait for it to terminate
agg_query = processed_df \
    .writeStream \
    .outputMode("append") \
    .foreachBatch(write_to_sqlite) \
    .start()

# Wait for the query to terminate
agg_query.awaitTermination()
