import json
import websocket
from kafka import KafkaProducer

KAFKA_BROKER = "localhost:9092"
KAFKA_TOPIC = "trades"
BINANCE_WS_URL = "wss://stream.binance.com:9443/ws/btcusdt@trade"

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

def on_message(ws, message):
    data = json.loads(message)
    
    trade = {
        "symbol": data["s"],
        "price": data["p"],
        "quantity": data["q"],
        "timestamp": data["T"]
    }
    
    producer.send(KAFKA_TOPIC, value=trade)
    print(f"Sent: {trade}")
    
    
if __name__ == "__main__":
    ws = websocket.WebSocketApp(BINANCE_WS_URL, on_message=on_message)
    ws.run_forever()