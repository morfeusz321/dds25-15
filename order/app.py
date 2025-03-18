import logging
import os
import atexit
import random
import uuid
from collections import defaultdict
import json

import redis
import requests
from kafka import KafkaProducer, KafkaConsumer
from kafka.errors import KafkaError

from msgspec import msgpack, Struct
from flask import Flask, jsonify, abort, Response


DB_ERROR_STR = "DB error"
REQ_ERROR_STR = "Requests error"

GATEWAY_URL = os.environ['GATEWAY_URL']


app = Flask("order-service")


db: redis.Redis = redis.Redis(host=os.environ['REDIS_HOST'],
                              port=int(os.environ['REDIS_PORT']),
                              password=os.environ['REDIS_PASSWORD'],
                              db=int(os.environ['REDIS_DB']))

"""
Setup Kafka producer
"""

KAFKA_BOOTSTRAP_SERVERS = os.environ.get('KAFKA_SERVERS', 'kafka1:19092').split(',')
STOCK_TOPIC = 'stock-topic'
PAYMENT_TOPIC = 'payment-topic'
ORDER_TOPIC = 'order-topic'

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    retries=5
)


def close_db_connection():
    db.close()
    producer.close()


atexit.register(close_db_connection)


class OrderValue(Struct):
    paid: bool
    items: list[tuple[str, int]]
    user_id: str
    total_cost: int


def get_order_from_db(order_id: str) -> OrderValue | None:
    try:
        # get serialized data
        entry: bytes = db.get(order_id)
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    # deserialize data if it exists else return null
    entry: OrderValue | None = msgpack.decode(entry, type=OrderValue) if entry else None
    if entry is None:
        # if order does not exist in the database; abort
        abort(400, f"Order: {order_id} not found!")
    return entry


@app.post('/create/<user_id>')
def create_order(user_id: str):
    key = str(uuid.uuid4())
    value = msgpack.encode(OrderValue(paid=False, items=[], user_id=user_id, total_cost=0))
    try:
        db.set(key, value)
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    return jsonify({'order_id': key})


@app.post('/batch_init/<n>/<n_items>/<n_users>/<item_price>')
def batch_init_users(n: int, n_items: int, n_users: int, item_price: int):

    n = int(n)
    n_items = int(n_items)
    n_users = int(n_users)
    item_price = int(item_price)

    def generate_entry() -> OrderValue:
        user_id = random.randint(0, n_users - 1)
        item1_id = random.randint(0, n_items - 1)
        item2_id = random.randint(0, n_items - 1)
        value = OrderValue(paid=False,
                           items=[(f"{item1_id}", 1), (f"{item2_id}", 1)],
                           user_id=f"{user_id}",
                           total_cost=2*item_price)
        return value

    kv_pairs: dict[str, bytes] = {f"{i}": msgpack.encode(generate_entry())
                                  for i in range(n)}
    try:
        db.mset(kv_pairs)
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    return jsonify({"msg": "Batch init for orders successful"})


@app.get('/find/<order_id>')
def find_order(order_id: str):
    order_entry: OrderValue = get_order_from_db(order_id)
    return jsonify(
        {
            "order_id": order_id,
            "paid": order_entry.paid,
            "items": order_entry.items,
            "user_id": order_entry.user_id,
            "total_cost": order_entry.total_cost
        }
    )


def send_post_request(url: str):
    try:
        response = requests.post(url)
    except requests.exceptions.RequestException:
        abort(400, REQ_ERROR_STR)
    else:
        return response


def send_get_request(url: str):
    try:
        response = requests.get(url)
    except requests.exceptions.RequestException:
        abort(400, REQ_ERROR_STR)
    else:
        return response


@app.post('/addItem/<order_id>/<item_id>/<quantity>')
def add_item(order_id: str, item_id: str, quantity: int):
    """
    This has to be changed to use the stock service to check if the item exists and if it does, add it to the order.
    """
    order_entry: OrderValue = get_order_from_db(order_id)
    item_reply = send_get_request(f"{GATEWAY_URL}/stock/find/{item_id}")
    if item_reply.status_code != 200:
        # Request failed because item does not exist
        abort(400, f"Item: {item_id} does not exist!")
    item_json: dict = item_reply.json()
    order_entry.items.append((item_id, int(quantity)))
    order_entry.total_cost += int(quantity) * item_json["price"]
    try:
        db.set(order_id, msgpack.encode(order_entry))
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    
    # Send request to stock service to update stock
    """
    This is a placeholder for the add_item function. It will send a message to the stock service with the stock change information.
    """
    producer.send(
        STOCK_TOPIC,
        key=order_id.encode(),
        value={
            'order_id': order_id,
            'item_id': item_id,
            'quantity': quantity
        }
    )


    return Response(f"Item: {item_id} added to: {order_id} price updated to: {order_entry.total_cost}",
                    status=200)


def rollback_stock(removed_items: list[tuple[str, int]]):
    for item_id, quantity in removed_items:
        send_post_request(f"{GATEWAY_URL}/stock/add/{item_id}/{quantity}")


@app.post('/checkout/<order_id>')
def checkout(order_id: str):
    # app.logger.debug(f"Checking out {order_id}")
    # order_entry: OrderValue = get_order_from_db(order_id)
    # # get the quantity per item
    # items_quantities: dict[str, int] = defaultdict(int)
    # for item_id, quantity in order_entry.items:
    #     items_quantities[item_id] += quantity
    # # The removed items will contain the items that we already have successfully subtracted stock from
    # # for rollback purposes.
    # removed_items: list[tuple[str, int]] = []
    # for item_id, quantity in items_quantities.items():
    #     stock_reply = send_post_request(f"{GATEWAY_URL}/stock/subtract/{item_id}/{quantity}")
    #     if stock_reply.status_code != 200:
    #         # If one item does not have enough stock we need to rollback
    #         rollback_stock(removed_items)
    #         abort(400, f'Out of stock on item_id: {item_id}')
    #     removed_items.append((item_id, quantity))
    # user_reply = send_post_request(f"{GATEWAY_URL}/payment/pay/{order_entry.user_id}/{order_entry.total_cost}")
    # if user_reply.status_code != 200:
    #     # If the user does not have enough credit we need to rollback all the item stock subtractions
    #     rollback_stock(removed_items)
    #     abort(400, "User out of credit")
    # order_entry.paid = True
    # try:
    #     db.set(order_id, msgpack.encode(order_entry))
    # except redis.exceptions.RedisError:
    #     return abort(400, DB_ERROR_STR)
    # app.logger.debug("Checkout successful")
    # return Response("Checkout successful", status=200)


    """
    This is a placeholder for the checkout function. It will send a message to the payment and stock services with their respective information.
    """

    producer.send(
        PAYMENT_TOPIC,
        key=order_id.encode(),
        value={
            'order_id': order_id
        }

    )

    producer.send(
        STOCK_TOPIC,
        key=order_id.encode(),
        value={
            'order_id': order_id
        }
    )
    
    print(f"Checkout successful for order: {order_id}")
    return Response("Checkout successful", status=200)


def process_order_event(value: dict):

    """
    This function will check if the order is paid and there is enough stock to fulfill the order.
    If the order is not paid but the stock has been subtracted, it will rollback the stock.
    and more.
    """
    print(f"Processing order event: {value}")

def start_order_consumer():
    consumer = KafkaConsumer(
        ORDER_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        group_id='order-group',
        auto_offset_reset='earliest'
    )

    for message in consumer:
        try:
            process_order_event(message.value)
        except Exception as e:
            app.logger.error(f"Error processing message: {message.value} - {e}")


"""
This will start the consumer in a separate thread so that it does not block the main thread.
"""
import threading
threading.Thread(target=start_order_consumer, daemon=True).start()

if __name__ == '__main__':
    app.run(host="0.0.0.0", port=8000, debug=True)
else:
    gunicorn_logger = logging.getLogger('gunicorn.error')
    app.logger.handlers = gunicorn_logger.handlers
    app.logger.setLevel(gunicorn_logger.level)
