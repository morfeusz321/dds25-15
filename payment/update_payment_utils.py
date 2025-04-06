import os
import redis
from msgspec import msgpack, Struct




class UserValue(Struct):
    credit: int

db: redis.Redis = redis.Redis(host=os.environ['REDIS_HOST'],
                              port=int(os.environ['REDIS_PORT']),
                              password=os.environ['REDIS_PASSWORD'],
                              db=int(os.environ['REDIS_DB']))

async def subtract_credits_with_lock(user_id: str, amount: int):
    lock = db.lock(f"lock:{user_id}", timeout=100)
    lock.acquire(blocking=True)
    try:

        user_entry = await db.get(user_id)
        if not user_entry:
            return f"User: {user_id} not found!", 404

        user_entry = msgpack.decode(user_entry, type=UserValue)
        if user_entry.credit < amount:
            return f"Not enough credit for user: {user_id}", 400

        user_entry.credit -= amount
        await db.set(user_id, msgpack.encode(user_entry))
        await db.hset(f"item:{user_id}", mapping={"stock": user_entry.credit})
        return f"User: {user_id} credit updated to: {user_entry.credit}", 200
    finally:
        lock.release()

async def add_credits_with_lock(user_id: str, amount: int):
    lock = db.lock(f"lock:{user_id}", timeout=100)
    lock.acquire(blocking=True)
    try:

        user_entry = await db.get(user_id)
        if not user_entry:
            return f"User: {user_id} not found!", 404

        user_entry = msgpack.decode(user_entry, type=UserValue)
        if user_entry.credit < amount:
            return f"Not enough credit for user: {user_id}", 400

        user_entry.credit += amount
        await db.set(user_id, msgpack.encode(user_entry))
        await db.hset(f"user:{user_id}", mapping={
            "credit": user_entry.credit,
        })
        return f"User: {user_id} credit updated to: {user_entry.credit}", 200
    finally:
        lock.release()

