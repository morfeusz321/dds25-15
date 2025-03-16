from kafka import KafkaConsumer
import json

KAFKA_BROKER = "localhost:9092"
TOPIC = "order-topic"

consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=KAFKA_BROKER,
    auto_offset_reset="earliest",
    enable_auto_commit=True,

)

print(f"Listening for messages on {TOPIC}...")
for message in consumer:
    print(f"Received message: {message}")