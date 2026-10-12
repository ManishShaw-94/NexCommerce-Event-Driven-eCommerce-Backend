import os

import redis
from dotenv import load_dotenv


load_dotenv()


redis_client = redis.Redis(
    host=os.getenv("REDIS_HOST"),
    port=int(os.getenv("REDIS_PORT", 6379)),
    decode_responses=True
)


def get_redis():
    return redis_client