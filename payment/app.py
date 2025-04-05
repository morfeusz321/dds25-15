import logging
import os
import atexit
import pickle
import uuid

import redis
from kafka import KafkaProducer, KafkaConsumer
from update_payment_utils import add_credits_with_lock, subtract_credits_with_lock
from msgspec import msgpack, Struct
from flask import Flask, jsonify, abort, Response

DB_ERROR_STR = "DB error"



app = Flask("payment-service")

app.logger.setLevel(logging.DEBUG)


db: redis.Redis = redis.Redis(host=os.environ['REDIS_HOST'],
                              port=int(os.environ['REDIS_PORT']),
                              password=os.environ['REDIS_PASSWORD'],
                              db=int(os.environ['REDIS_DB']))

"""
Setup Kafka producer
"""

KAFKA_BOOTSTRAP_SERVERS = os.environ.get('KAFKA_SERVERS', 'kafka1:19092').split(',')
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


class UserValue(Struct):
    credit: int

class OrderValue(Struct):
    paid: bool
    items: list[tuple[str, int]]
    user_id: str
    total_cost: int


def get_user_from_db(user_id: str) -> UserValue | None:
    try:
        # get serialized data
        entry: bytes = db.get(user_id)
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    # deserialize data if it exists else return null
    entry: UserValue | None = msgpack.decode(entry, type=UserValue) if entry else None
    if entry is None:
        # if user does not exist in the database; abort
        abort(400, f"User: {user_id} not found!")
    return entry


@app.post('/create_user')
def create_user():
    key = str(uuid.uuid4())
    value = msgpack.encode(UserValue(credit=0))
    try:
        db.set(key, value)
        db.hset(f"user:{key}", mapping={
            "credit": 0,
        })
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    return jsonify({'user_id': key})


@app.post('/batch_init/<n>/<starting_money>')
def batch_init_users(n: int, starting_money: int):
    n = int(n)
    starting_money = int(starting_money)
    kv_pairs: dict[str, bytes] = {f"{i}": msgpack.encode(UserValue(credit=starting_money))
                                  for i in range(n)}
    try:
        db.mset(kv_pairs)
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    return jsonify({"msg": "Batch init for users successful"})


@app.get('/find_user/<user_id>')
def find_user(user_id: str):
    user_entry: UserValue = get_user_from_db(user_id)
    return jsonify(
        {
            "user_id": user_id,
            "credit": user_entry.credit
        }
    )


@app.post('/add_funds/<user_id>/<amount>')
async def add_credit(user_id: str, amount: int):
    # user_entry: UserValue = get_user_from_db(user_id)
    # # update credit, serialize and update database
    # user_entry.credit += int(amount)
    # try:
    #     db.set(user_id, msgpack.encode(user_entry))
    #     db.hset(f"user:{user_id}", mapping={
    #         "credit": user_entry.credit,
    #     })
    # except redis.exceptions.RedisError:
    #     return abort(400, DB_ERROR_STR)
    # return Response(f"User: {user_id} credit updated to: {user_entry.credit}", status=200)
    await add_credits_with_lock(user_id, amount)

@app.post('/pay/<user_id>/<amount>')
async def remove_credit(user_id: str, amount: int):
    # app.logger.debug(f"Removing {amount} credit from user: {user_id}")
    # user_entry: UserValue = get_user_from_db(user_id)
    # # update credit, serialize and update database
    # user_entry.credit -= int(amount)
    # if user_entry.credit < 0:
    #     abort(400, f"User: {user_id} credit cannot get reduced below zero!")
    # try:
    #     db.set(user_id, msgpack.encode(user_entry))
    #     db.hset(f"user:{user_id}", mapping={
    #         "credit": user_entry.credit,
    #     })
    # except redis.exceptions.RedisError:
    #     return abort(400, DB_ERROR_STR)
    # return Response(f"User: {user_id} credit updated to: {user_entry.credit}", status=200)
    await subtract_credits_with_lock(user_id, amount)

"""
Here we process the payment event and send the payment status to the order service.
"""
async def process_payment_event(message):
    order_id, order = message.value

    if message.key == "make_payment":
        try:
            await remove_credit(order.user_id, order.total_cost)
        except Exception as e:
            print(f"Error removing credit: {e}")
            producer.send(ORDER_TOPIC, key="payment_failed", value=(order_id, order))
            return abort(400, f"Error removing credit: {e}")
        
        producer.send(ORDER_TOPIC, key="payment_made", value=(order_id, order))

    elif message.key == "rollback_payment":
        try:
            add_credit(order.user_id, order.total_cost)
            app.logger.info(f"Credit rolled back for order: {order_id}")
        except Exception as e:
            #TODO: in this case we should just retry no need for any other rollback
            return abort(400, f"Error rolling back credit: {e}")


def start_payment_consumer():
    consumer = KafkaConsumer(
        PAYMENT_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id='payment-group',
        auto_offset_reset='earliest',
        key_deserializer=lambda k: pickle.loads(k),
        value_deserializer=lambda v: pickle.loads(v)
    )

    for message in consumer:
        try:
            process_payment_event(message)
        except Exception as e:
            app.logger.error(f"Error processing payment event: {e.__cause__}")

"""
This creates a new thread to start the payment consumer so it does not block the main thread.
"""
import threading
threading.Thread(target=start_payment_consumer, daemon=True).start()

if __name__ == '__main__':
    app.run(host="0.0.0.0", port=8000, debug=True)
else:
    gunicorn_logger = logging.getLogger('gunicorn.error')
    app.logger.handlers = gunicorn_logger.handlers
    app.logger.setLevel(gunicorn_logger.level)