from pyspark.sql import SparkSession
from pyspark.sql.functions import col, window, avg
import psycopg2

KAFKA_TOPIC = "crypto_prices"
BROKER = "localhost:9092"
DB_CONFIG = {"dbname": "crypto_db", "user": "postgres", "password": "password", "host": "localhost", "port": 5432}

spark = SparkSession.builder.appName("CryptoStream").getOrCreate()
df = spark.readStream.format("kafka").option("kafka.bootstrap.servers", BROKER).option("subscribe", KAFKA_TOPIC).load()
df = df.selectExpr("CAST(value AS STRING)").selectExpr("from_json(value, 'symbol STRING, price DOUBLE, timestamp DOUBLE') as data").select("data.*")

moving_avg = df.withWatermark("timestamp", "1 minute").groupBy("symbol", window("timestamp", "30 seconds")).agg(avg("price").alias("avg_price"))

def write_to_db(batch_df, batch_id):
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    for row in batch_df.collect():
        cur.execute("INSERT INTO crypto_prices (symbol, avg_price, timestamp) VALUES (%s, %s, NOW())", (row["symbol"], row["avg_price"]))
    conn.commit()
    cur.close()
    conn.close()

moving_avg.writeStream.foreachBatch(write_to_db).outputMode("update").start().awaitTermination()
