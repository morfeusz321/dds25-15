from kafka import KafkaConsumer
import json


"""
Initial messages using postman, curl or other tools
Example: http://127.0.0.1:8000/orders/checkout/124356
After the effect can be seen of the message queue.
"""

KAFKA_BROKER = "localhost:9092"
TOPIC = "order-topic"
# TOPIC = "payment-topic"
# TOPIC = "stock-topic"

consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=KAFKA_BROKER,
    auto_offset_reset="earliest",
    enable_auto_commit=True,

)

print(f"Listening for messages on {TOPIC}...")
for message in consumer:
    print(f"Received message: {message}")