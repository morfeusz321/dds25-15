import logging
import os
import atexit
import random
import uuid
import pickle
import threading
import redis
import requests

from kafka import KafkaProducer, KafkaConsumer
from msgspec import msgpack, Struct
from flask import Flask, jsonify, abort, Response
from order_state_manipultion import *
from time import sleep

def wait_until_redis_alive(db, poll_interval=1):
    while True:
        print("Waiting for Redis to be alive...")
        try:
            db.ping()
            break
        except redis.exceptions.RedisError:
            sleep(poll_interval)


'''intial setup'''

DB_ERROR_STR = "DB error"
REQ_ERROR_STR = "Requests error"
GATEWAY_URL = os.environ['GATEWAY_URL']
app = Flask("order-service")

# temporary hashmap to keep track of order responses cannot be used if we want crash tolerance for order service, 
# or if order is going to have multiple instances
# indemopotency key 

db: redis.Redis = redis.Redis(host=os.environ['REDIS_HOST'],
                              port=int(os.environ['REDIS_PORT']),
                              password=os.environ['REDIS_PASSWORD'],
                              db=int(os.environ['REDIS_DB']))

# Create a table in redis to keep track of the state of checkout orders
# 3 columns order_id, stock_subtracted, payment_made
# order_id is the key, stock_subtracted and payment_made are ints -1, 0, 1
# -1 means no message was received, 0 means failure, 1 means success

# add table to redis

#TODO code here



"""
Setup Kafka producer
"""

KAFKA_BOOTSTRAP_SERVERS = os.environ.get('KAFKA_SERVERS', 'kafka1:19092').split(',')
STOCK_TOPIC = 'stock-topic'
PAYMENT_TOPIC = 'payment-topic'
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
    Add item does not need to communicate with any other services we decided to just check the availability of the item
    at the checkout.
    """
    order_entry: OrderValue = get_order_from_db(order_id)
    item_reply = send_get_request(f"{GATEWAY_URL}/stock/find/{item_id}")
    if item_reply.status_code != 200:
        # Request failed because item does not exist
        abort(400, f"Item: {item_id} does not exist!")
    item_json: dict = item_reply.json()
    #TODO: check if item is already in order and update quantity instead of adding a new item with the same id
    order_entry.items.append((item_id, int(quantity)))
    order_entry.total_cost += int(quantity) * item_json["price"]
    try:
        db.set(order_id, msgpack.encode(order_entry))
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    return Response(f"Item: {item_id} added to: {order_id} price updated to: {order_entry.total_cost}",
                    status=200)


def rollback_stock(removed_items: list[tuple[str, int]]):
    for item_id, quantity in removed_items:
        send_post_request(f"{GATEWAY_URL}/stock/add/{item_id}/{quantity}")


@app.post('/checkout/<order_id>')
def checkout(order_id: str):

    wait_until_redis_alive(db)

    order_entry: OrderValue = get_order_from_db(order_id)

    if order_entry.paid:
        abort(400, f"Order: {order_id} has already been paid!")

    #send payment event with the amount to payment service
    producer.send(
        PAYMENT_TOPIC,
        key="make_payment",
        value=(order_id, order_entry)
    )

    #send stock event with the items to stock service
    producer.send(
        STOCK_TOPIC,
        key="subtract_stock",
        value=(order_id, order_entry)
    )
    
    return Response(f"Checkout for order {order_id} is processing...", status=200)


"""
This function will check if the order is paid and there is enough stock to fulfill the order.
If the order is not paid but the stock has been subtracted, it will rollback the stock.
and more.
"""

def process_order_event(message):
    # print thread name
    order_id, order = message.value

    # try to initialize the order state
    
    msg = init_checkout_state(db, order_id)
    
    print(msg)

    if message.key == "stock_subtracted":
        update_checkout_statedb(db, order_id, "stock_subtracted", 1)
        print("Stock subtracted and state " + str(get_check_state(db, order_id)))
        if get_check_state(db, order_id)["payment_made"] == 1:
            print("Payment made")
            order.paid = True
            db.set(order_id, msgpack.encode(order))
            print(f"Order: {order_id} completed")
        elif get_check_state(db, order_id)["payment_made"] == 0:
            print("Payment not made rolling back stock")
            producer.send(STOCK_TOPIC, key="rollback_stock", value=(order_id, order))



    elif message.key == "payment_made":
        update_checkout_statedb(db, order_id, "payment_made", 1)
        print("Payment made and state " + str(get_check_state(db, order_id)))

        if get_check_state(db, order_id)["stock_subtracted"] == 1:
            order.paid = True
            db.set(order_id, msgpack.encode(order))
            print(f"Order: {order_id} completed")
        elif get_check_state(db, order_id)["stock_subtracted"] == 0:
            print("Stock not subtracted rolling back payment")
            producer.send(PAYMENT_TOPIC, key="rollback_payment", value=(order_id, order))



    elif message.key == "stock_subtraction_failed":
        update_checkout_statedb(db, order_id, "stock_subtracted", 0)
        print("Stock subtraction failed")
        if get_check_state(db, order_id)["payment_made"] == 1:
            producer.send(PAYMENT_TOPIC, key="rollback_payment", value=(order_id, order))
    elif message.key == "payment_failed":
        update_checkout_statedb(db, order_id, "payment_made", 0)
        print("Payment failed")
        if get_check_state(db, order_id)["stock_subtracted"] == 1:
            producer.send(STOCK_TOPIC, key="rollback_stock", value=(order_id, order))

            

def start_order_consumer():
    consumer = KafkaConsumer(
        ORDER_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        key_deserializer=lambda k: pickle.loads(k),
        value_deserializer=lambda v: pickle.loads(v),
        group_id='order-group',
        auto_offset_reset='earliest'
    )

    for message in consumer:
        # print the number of threads
        try:
            # print("NUMBER OF ACTIVE THREADS ", threading.active_count())
            process_order_event(message)
        except Exception as e:
            app.logger.error(f"Error processing order event: {e.__cause__}")


"""
This will start the consumer in a separate thread so that it does not block the main thread.
"""

# threading.Thread(target=start_order_consumer, daemon=True, ).start()
# start_order_consumer() with one single thread
t1 = threading.Thread(target=start_order_consumer)

t1.start()


# start_order_consumer()

if __name__ == '__main__':
    app.run(host="0.0.0.0", port=8000, debug=True)
else:
    gunicorn_logger = logging.getLogger('gunicorn.error')
    app.logger.handlers = gunicorn_logger.handlers
    app.logger.setLevel(gunicorn_logger.level)
