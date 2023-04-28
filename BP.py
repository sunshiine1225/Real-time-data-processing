import os
import urllib.request
import sqlite3
from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder.appName("BatchProcessing").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# Download SQLite JDBC driver
driver_url = "https://repo1.maven.org/maven2/org/xerial/sqlite-jdbc/3.36.0.1/sqlite-jdbc-3.36.0.1.jar"
driver_filename = os.path.basename(driver_url)
temp_dir = "/tmp"
driver_path = os.path.join(temp_dir, driver_filename)
urllib.request.urlretrieve(driver_url, driver_path)

# Connect to SQLite database
conn = sqlite3.connect("w60s.db")

# Query data from tables and load into Spark DataFrame
ethereum_df = spark.read.jdbc("jdbc:sqlite:w60s.db", "ethereum_60s")
dogecoin_df = spark.read.jdbc("jdbc:sqlite:w60s.db", "dogecoin_60s")
tether_df = spark.read.jdbc("jdbc:sqlite:w60s.db", "tether_60s")
bitcoin_df = spark.read.jdbc("jdbc:sqlite:w60s.db", "bitcoin_60s")
idex_df = spark.read.jdbc("jdbc:sqlite:w60s.db", "idex_60s")
goldmaxcoin_df = spark.read.jdbc("jdbc:sqlite:w60s.db", "goldmaxcoin_60s")

# Register DataFrames as temporary tables
ethereum_df.createOrReplaceTempView("ethereum")
dogecoin_df.createOrReplaceTempView("dogecoin")
tether_df.createOrReplaceTempView("tether")
bitcoin_df.createOrReplaceTempView("bitcoin")
idex_df.createOrReplaceTempView("idex")
goldmaxcoin_df.createOrReplaceTempView("goldmaxcoin")

# Query average price and volume for each base
query = """
SELECT base, AVG(price) as avg_price, SUM(volume) as total_volume
FROM (
  SELECT base, price, volume
  FROM ethereum
  UNION ALL
  SELECT base, price, volume
  FROM dogecoin
  UNION ALL
  SELECT base, price, volume
  FROM tether
  UNION ALL
  SELECT base, price, volume
  FROM bitcoin
  UNION ALL
  SELECT base, price, volume
  FROM idex
  UNION ALL
  SELECT base, price, volume
  FROM goldmaxcoin
)
GROUP BY base
"""

# Execute query and show results
result_df = spark.sql(query)
result_df.show()
