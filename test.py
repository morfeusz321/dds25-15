from kafka import KafkaProducer, KafkaConsumer
import json
import time



def test_kafka():
    print("Creating producer")
    producer = KafkaProducer(bootstrap_servers=['127.0.0.1:9092'], value_serializer=lambda v: json.dumps(v).encode('utf-8'))

    print("Sending message")
    producer.send('test', {'test': 'message', 'time': time.time()})
    producer.flush()
    print("Message sent")

    print("Creating consumer")
    consumer = KafkaConsumer(
        'test',
        bootstrap_servers=['127.0.0.1:9092'],
        auto_offset_reset='earliest',
        group_id='test-group',
        value_deserializer=lambda m: json.loads(m.decode('utf-8'))
    )

    print("Waiting for message (timeout 10s)")

    start_time = time.time()
    message_received = False

    while time.time() - start_time < 10:
        records = consumer.poll(timeout_ms=1000)
        if records:
            for tp, messages in records.items():
                for message in messages:
                    print(message.value)
                    message_received = True
                    break
            if message_received:
                break
        time.sleep(0.5)
    
    if not message_received:
        print("No message received")
    else:
        print("Message received")
    
    print("Closing producer")
    producer.close()
    print("Closing consumer")
    consumer.close()

if __name__ == '__main__':
    test_kafka()
