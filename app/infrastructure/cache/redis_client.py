import redis.asyncio as redis

redis_client = redis.Redis(
    host="localhost",
    port=6378,
    decode_responses=True,
)