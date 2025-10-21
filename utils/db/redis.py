import logging
import os

import redis
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger("RedisService")
logging.basicConfig(level=logging.INFO)


class RedisService:
    instance = None

    def __new__(cls, redis_url=None):
        if cls.instance:
            return cls.instance
        obj = super().__new__(cls)
        cls.instance = obj
        return obj

    def __init__(self, redis_url=None):
        if getattr(self, "_initialized", False):
            return

        self.redis_url = redis_url or os.getenv("REDIS_URL")
        if not self.redis_url:
            raise ValueError("Missing REDIS_URL in environment")

        self.client = None
        self.connected = False
        self._initialized = True

    def connect(self):
        try:
            self.client = redis.Redis.from_url(self.redis_url, decode_responses=True)
            self.client.ping()
            self.connected = True
            logger.info("Connected to Redis")
        except redis.RedisError as e:
            self.connected = False
            logger.error(f"Redis connection failed: {e}")

    def is_connected(self) -> bool:
        if not self.client:
            return False
        try:
            self.client.ping()
            return True
        except Exception:
            return False

    def ensure_connection(self):
        if not self.is_connected():
            logger.warning("Redis connection lost. Reconnecting...")
            self.connect()

    def safe_get(self, key: str):
        try:
            self.ensure_connection()
            if not self.client:
                return None
            return self.client.get(key)
        except Exception as e:
            logger.warning(f"Redis get failed: {e}")
            return None

    def safe_set(self, key: str, value, ex: int = None):
        try:
            self.ensure_connection()
            if not self.client:
                return
            self.client.set(key, value, ex=ex)
        except Exception as e:
            logger.warning(f"Redis set failed: {e}")
