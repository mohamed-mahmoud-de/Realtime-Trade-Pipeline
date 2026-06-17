import os
os.environ["HADOOP_HOME"] = r"C:\hadoop"
os.environ["JAVA_HOME"] = r"C:\Program Files\Eclipse Adoptium\jdk-17.0.19.10-hotspot"
os.environ["PATH"] = r"C:\hadoop\bin" + ";" + os.environ.get("PATH", "")

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, LongType
from pyspark.sql.functions import window, avg, count
from pyspark.sql.types import TimestampType

spark = SparkSession.builder \
    .appName("TradeStreamProcessor") \
    .master("local[*]") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.2,com.datastax.spark:spark-cassandra-connector_2.13:3.5.1") \
    .config("spark.cassandra.connection.host", "localhost") \
    .getOrCreate()
    
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "trades") \
    .option("startingOffsets", "latest") \
    .load()
    
raw_trades = df.select(
    col("value").cast("string").alias("raw_json")
)

trade_schema = StructType([
    StructField("symbol", StringType()),
    StructField("price", StringType()),
    StructField("quantity", StringType()),
    StructField("timestamp", LongType())
])

parsed_trades = raw_trades.select(
    from_json(col("raw_json"), trade_schema).alias("trade")
)

flattened_trades = parsed_trades.select(
    col("trade.symbol").alias("symbol"),
    col("trade.price").alias("price"),
    col("trade.quantity").alias("quantity"),
    col("trade.timestamp").alias("timestamp")
)

trades_with_time = flattened_trades.withColumn(
    "event_time",
    (col("timestamp") / 1000).cast(TimestampType())
).withColumn(
    "price_numeric",
    col("price").cast("double")
)

windowed_avg = trades_with_time \
    .withWatermark("event_time", "2 minutes") \
    .groupBy(
        window(col("event_time"), "1 minute"),
        col("symbol")
    ) \
    .agg(
        avg(col("price_numeric")).alias("avg_price"),
        count(col("price_numeric")).alias("trade_count")
    )

parsed_trades.printSchema()
flattened_trades.printSchema()


query = windowed_avg.writeStream \
    .outputMode("update") \
    .format("console") \
    .option("checkpointLocation", "checkpoints/trades_windowed") \
    .option("truncate", "false") \
    .start()
    
cassandra_df = windowed_avg.select(
    col("symbol"),
    col("window.start").alias("window_start"),
    col("window.end").alias("window_end"),
    col("avg_price"),
    col("trade_count")
)

cassandra_query = cassandra_df.writeStream \
    .outputMode("update") \
    .foreachBatch(lambda batch_df, batch_id: 
        batch_df.write \
            .format("org.apache.spark.sql.cassandra") \
            .option("keyspace", "trade_pipeline") \
            .option("table", "windowed_trades") \
            .mode("append") \
            .save()
    ) \
    .option("checkpointLocation", "checkpoints/trades_cassandra") \
    .start()


spark.streams.awaitAnyTermination()