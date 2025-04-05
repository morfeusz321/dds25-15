import os
import redis
from msgspec import msgpack, Struct


class StockValue(Struct):
    stock: int
    price: int

db: redis.Redis = redis.Redis(host=os.environ['REDIS_HOST'],
                              port=int(os.environ['REDIS_PORT']),
                              password=os.environ['REDIS_PASSWORD'],
                              db=int(os.environ['REDIS_DB']))

async def subtract_stock_with_lock(item_id: str, amount: int):
    lock = db.lock(f"lock:{item_id}", timeout=100)
    lock.acquire(blocking=True)
    try:

        item_entry = await db.get(item_id)
        if not item_entry:
            return f"Item: {item_id} not found!", 404

        item_entry = msgpack.decode(item_entry, type=StockValue)
        if item_entry.stock < amount:
            return f"Not enough stock for item: {item_id}", 400

        item_entry.stock -= amount
        await db.set(item_id, msgpack.encode(item_entry))
        await db.hset(f"item:{item_id}", mapping={"stock": item_entry.stock})
        return f"Item: {item_id} stock updated to: {item_entry.stock}", 200
    finally:
        lock.release()

async def add_stock_with_lock(item_id: str, amount: int):
    lock = db.lock(f"lock:{item_id}", timeout=100)
    lock.acquire(blocking=True)
    try:

        item_entry = await db.get(item_id)
        if not item_entry:
            return f"Item: {item_id} not found!", 404

        item_entry = msgpack.decode(item_entry, type=StockValue)
        if item_entry.stock < amount:
            return f"Not enough stock for item: {item_id}", 400

        item_entry.stock += amount
        await db.set(item_id, msgpack.encode(item_entry))
        await db.hset(f"item:{item_id}", mapping={"stock": item_entry.stock})
        return f"Item: {item_id} stock updated to: {item_entry.stock}", 200
    finally:
        lock.release()

