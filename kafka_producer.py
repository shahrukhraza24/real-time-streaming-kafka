from kafka import KafkaProducer
import json
import requests
import time

KAFKA_TOPIC = "crypto_prices"
BROKER = "localhost:9092"
producer = KafkaProducer(bootstrap_servers=BROKER, value_serializer=lambda x: json.dumps(x).encode("utf-8"))

while True:
    response = requests.get("https://api.binance.com/api/v3/ticker/price?symbol=BTCUSDT").json()
    data = {"symbol": response["symbol"], "price": float(response["price"]), "timestamp": time.time()}
    producer.send(KAFKA_TOPIC, value=data)
    print(f"Sent: {data}")
    time.sleep(1)
