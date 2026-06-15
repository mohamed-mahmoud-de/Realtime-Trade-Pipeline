from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, LongType

spark = SparkSession.builder \
    .appName("TradeStreamProcessor") \
    .master("local[*]") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.2") \
    .getOrCreate()
    
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "trades") \
    .option("startingOffsets", "earliest") \
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

parsed_trades.printSchema()
flattened_trades.printSchema()


query = flattened_trades.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("checkpointLocation", "checkpoints/trades_console") \
    .start()


query.awaitTermination()