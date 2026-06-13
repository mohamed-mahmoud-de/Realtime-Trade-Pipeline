import json
from kafka import KafkaConsumer

KAFKA_BROKER = "localhost:9092"
KAFKA_TOPIC = "trades"
GROUP_ID = "trade-consumers"

consumer = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers=KAFKA_BROKER,
    group_id=GROUP_ID,
    value_deserializer=lambda x: json.loads(x.decode('utf-8')),
    auto_offset_reset='earliest'
)

if __name__ == "__main__":
    print(f"Listening to topic '{KAFKA_TOPIC}' as group '{GROUP_ID}'...")
    
    for message in consumer:
        print(f"Partition: {message.partition} | Offset: {message.offset} | Data: {message.value}")