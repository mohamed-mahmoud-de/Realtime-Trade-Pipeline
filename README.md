# Real-Time Crypto Trade Pipeline

A streaming data pipeline that ingests live cryptocurrency trades from Binance, processes them in real time using tumbling window aggregations, and writes results to both Cassandra and Elasticsearch for different query patterns. Visualized through Kibana dashboards.

This is not a batch job that runs on a schedule — it processes every trade as it happens, continuously.

## Architecture

![Pipeline Architecture](Realtime%20Trade%20Pipeline.png)

## Why These Tools

**Kafka** — Acts as a buffer between the data source and Spark. If Spark goes down, trades don't disappear — Kafka holds them until Spark recovers and resumes from the exact offset where it stopped. It also decouples the producer from the consumer: you could add a second consumer (fraud detection, alerting) without changing a single line in the producer.

**Spark Structured Streaming** — Processes the stream as an unbounded table. Tumbling windows group trades into 1-minute buckets and compute avg_price and trade_count per window per symbol — reducing thousands of individual trades into meaningful summaries. Watermarks (2 minutes) allow late-arriving trades to still be counted in the correct window, trading memory for correctness.

**Cassandra** — Streaming writes need a storage engine that doesn't slow down under constant inserts. Postgres uses row-level locking — every INSERT waits for a lock. Cassandra writes to an append-only log in memory (memtable) with no locks, then flushes to disk later. The tradeoff is eventual consistency, which is fine for analytics aggregations. Data is partitioned by symbol and clustered by window_start, optimized for the query: "give me all windows for BTCUSDT in the last hour."

**Elasticsearch** — Cassandra is fast but rigid — you must query by partition key (symbol). Elasticsearch uses an inverted index, so you can search by any field: "all windows where avg_price > 95000" or "windows with trade_count above 500." Two storage systems because we have two types of questions.

**Kibana** — Sits on top of Elasticsearch. Turns the indexed data into dashboards — price trends over time, trade volume per minute. Makes the pipeline demonstrable without writing queries by hand.

## Project Structure

```
realtime-trade-pipeline/
├── docker/
│   └── docker-compose.yml    # ZooKeeper, Kafka, Cassandra, ES, Kibana
├── producer/
│   └── producer.py           # Binance WebSocket → Kafka
├── consumer/
│   └── consumer.py           # Simple Kafka consumer (debugging)
├── spark/
│   └── spark_job.py          # Streaming job: Kafka → Spark → Cassandra + ES
├── checkpoints/              # Spark checkpoint data (gitignored)
├── requirements.txt
└── CLAUDE.md                 # Full project journal with session notes
```

## How to Run

### Prerequisites
- Docker and Docker Compose
- Python 3.x with pip
- Java 17 (not 25 — incompatible with Spark 4.x)
- `winutils.exe` at `C:\hadoop\bin\` (Windows only)

### 1. Start the infrastructure
```bash
cd docker
docker-compose up -d
```
Wait ~30 seconds for all services to be healthy. Cassandra takes the longest.

### 2. Create the Kafka topic
```bash
docker exec kafka kafka-topics --create --topic trades --partitions 3 --replication-factor 1 --bootstrap-server localhost:9092
```

### 3. Create the Cassandra keyspace and table
```bash
docker exec -it cassandra cqlsh
```
```sql
CREATE KEYSPACE trade_pipeline WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

CREATE TABLE trade_pipeline.windowed_trades (
    symbol TEXT,
    window_start TIMESTAMP,
    window_end TIMESTAMP,
    avg_price DOUBLE,
    trade_count BIGINT,
    PRIMARY KEY (symbol, window_start)
);
```

### 4. Install Python dependencies
```bash
pip install -r requirements.txt
```

### 5. Start the producer
```bash
python producer/producer.py
```

### 6. Start the Spark streaming job (in a separate terminal)
```bash
python spark/spark_job.py
```

### 7. Verify data
- **Cassandra:** `docker exec -it cassandra cqlsh -e "SELECT * FROM trade_pipeline.windowed_trades LIMIT 5;"`
- **Elasticsearch:** `curl localhost:9200/windowed_trades/_search?pretty`
- **Kibana:** Open `http://localhost:5601`

## What I Learned

**Kafka is a buffer, not just a message queue.** I initially thought of Kafka as a pipe — data goes in, data comes out. The real value is what happens when things break. I killed Kafka while Spark was running. When Kafka came back, Spark resumed from the exact checkpoint offset. No data loss. That's when "fault tolerance" stopped being a buzzword.

**Two databases for two question types.** I didn't understand why we needed both Cassandra and Elasticsearch until I tried querying Cassandra for "all windows where avg_price > 95000." You can't — the partition key is symbol, and Cassandra only does fast lookups by partition key. Elasticsearch's inverted index lets you search by any field. Same data, different access patterns.

**Spark 4.x compatibility is painful.** The Elasticsearch Spark connector JAR didn't work with Spark 4.x (NoSuchMethodError). I had to switch to the Python elasticsearch library and write to ES inside foreachBatch instead. I also discovered that running two separate writeStreams from the same Kafka source causes a NullPointerException — a known Spark 4.1.2 bug. The fix was merging both sinks into a single foreachBatch function.

**Shuffle partitions matter.** Spark defaults to 200 shuffle partitions. With a local single-machine setup processing one Kafka topic, that meant 199 empty tasks per batch doing nothing. Setting `spark.sql.shuffle.partitions=4` made every batch noticeably faster. Default settings are designed for clusters, not local development.

**Watermarks are a memory vs correctness tradeoff.** Without a watermark, Spark either keeps every window open forever (memory explodes) or drops late data silently. A 2-minute watermark says: accept late trades up to 2 minutes, then close the window. For crypto data that rarely arrives more than seconds late, this is generous — but it's a conscious decision, not a default.

## Tech Stack

| Component | Version | Purpose |
|-----------|---------|---------|
| Apache Kafka | Confluent 7.4.0 | Event buffering and transport |
| Apache Spark | 4.1.2 (Scala 2.13) | Stream processing with windowed aggregations |
| Apache Cassandra | 4.1 | Low-latency operational writes |
| Elasticsearch | 8.11.0 | Flexible search and aggregation |
| Kibana | 8.11.0 | Dashboard visualization |
| Python | 3.x | Producer, consumer, orchestration |
| Docker Compose | - | Service orchestration |
