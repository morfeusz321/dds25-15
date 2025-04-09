import logging
import os
import atexit
import uuid
import pickle

import redis
from kafka import KafkaProducer, KafkaConsumer
from kafka.errors import KafkaError
from time import sleep

from msgspec import msgpack, Struct
from flask import Flask, jsonify, abort, Response


def wait_until_redis_alive(db, poll_interval=1):
    while True:
        try:
            db.ping()
            break
        except redis.exceptions.RedisError:
            sleep(poll_interval)



DB_ERROR_STR = "DB error"

app = Flask("stock-service")

db: redis.Redis = redis.Redis(host=os.environ['REDIS_HOST'],
                              port=int(os.environ['REDIS_PORT']),
                              password=os.environ['REDIS_PASSWORD'],
                              db=int(os.environ['REDIS_DB']))


"""
Setup Kafka producer
"""

KAFKA_BOOTSTRAP_SERVERS = os.environ.get('KAFKA_SERVERS', 'kafka1:19092').split(',')
STOCK_TOPIC = 'stock-topic'
ORDER_TOPIC = 'order-topic'

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    key_serializer=lambda k: pickle.dumps(k),
    value_serializer=lambda v: pickle.dumps(v),
    retries=5
)

def close_db_connection():
    db.close()
    producer.close()


atexit.register(close_db_connection)


class StockValue(Struct):
    stock: int
    price: int

class OrderValue(Struct):
    paid: bool
    items: list[tuple[str, int]]
    user_id: str
    total_cost: int


def get_item_from_db(item_id: str) -> StockValue | None:
    try:
        # Retrieve all fields of the hash
        entry = db.hgetall(f"item:{item_id}")
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    
    if not entry:
        # If item does not exist in the database; abort
        abort(400, f"Item: {item_id} not found!")
    
    # Deserialize the hash fields into a StockValue object
    return StockValue(
        stock=int(entry[b'stock']),
        price=int(entry[b'price'])
    )


@app.post('/item/create/<price>')
def create_item(price: int):
    key = str(uuid.uuid4())
    try:
        db.hset(f"item:{key}", mapping={
            "stock": 0,
            "price": int(price)
        })
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    return jsonify({'item_id': key})


@app.post('/batch_init/<n>/<starting_stock>/<item_price>')
def batch_init_users(n: int, starting_stock: int, item_price: int):
    n = int(n)
    starting_stock = int(starting_stock)
    item_price = int(item_price)
    kv_pairs: dict[str, bytes] = {f"{i}": msgpack.encode(StockValue(stock=starting_stock, price=item_price))
                                  for i in range(n)}
    try:
        db.mset(kv_pairs)
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    return jsonify({"msg": "Batch init for stock successful"})


@app.get('/find/<item_id>')
def find_item(item_id: str):
    item_entry: StockValue = get_item_from_db(item_id)
    return jsonify(
        {
            "stock": item_entry.stock,
            "price": item_entry.price
        }
    )


@app.post('/add/<item_id>/<amount>')
def add_stock(item_id: str, amount: int):
    wait_until_redis_alive(db)

    item_entry: StockValue = get_item_from_db(item_id)
    item_entry.stock += int(amount)

    try:
        db.hset(f"item:{item_id}", mapping={
            "stock": item_entry.stock,
            "price": item_entry.price
        })
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    return Response(f"Item: {item_id} stock updated to: {item_entry.stock}", status=200)


@app.post('/subtract/<item_id>/<amount>')
def remove_stock(item_id: str, amount: int):
    wait_until_redis_alive(db)

    item_entry: StockValue = get_item_from_db(item_id)
    item_entry.stock -= int(amount)
    if item_entry.stock < 0:
        abort(400, f"Item: {item_id} stock cannot get reduced below zero!")

    try:
        db.hset(f"item:{item_id}", mapping={
            "stock": item_entry.stock,
            "price": item_entry.price
        })
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    return Response(f"Item: {item_id} stock updated to: {item_entry.stock}", status=200)


def process_stock_event(message):
    order_id, order = message.value

    if message.key == "subtract_stock":
        try:
            for item_id, amount in order.items:
                #TODO: make sure that if some get removed but not all then rollback locally
                remove_stock(item_id, amount)
        except Exception as e:
            producer.send(ORDER_TOPIC, key="stock_subtraction_failed", value=(order_id, order))
            return abort(400, f"Error subtracting stock: {e}")

        producer.send(ORDER_TOPIC, key="stock_subtracted", value=(order_id, order))

    elif message.key == "rollback_stock":
        try:
            for item_id, amount in order.items:
                add_stock(item_id, amount)
                app.logger.info(f"Stock rolled back for order: {order_id}")
        except Exception as e:
            #TODO: in this case we should just retry no need for any other rollback but be sure to only retry the failed items
            return abort(400, f"Error rolling back stock: {e}")
        
def start_stock_consumer():
    consumer = KafkaConsumer(
        STOCK_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id='stock-group',
        auto_offset_reset='earliest',
        key_deserializer=lambda k: pickle.loads(k),
        value_deserializer=lambda v: pickle.loads(v)
    )

    for message in consumer:
        try:
            process_stock_event(message)
        except Exception as e:
            app.logger.error(f"Error processing stock event: {e.__cause__}")

"""
Start the stock consumer in a separate thread so it does not block the main thread.
"""
import threading
threading.Thread(target=start_stock_consumer, daemon=True).start()

if __name__ == '__main__':
    app.run(host="0.0.0.0", port=8000, debug=True)
else:
    gunicorn_logger = logging.getLogger('gunicorn.error')
    app.logger.handlers = gunicorn_logger.handlers
    app.logger.setLevel(gunicorn_logger.level)
