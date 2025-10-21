import logging
import os

from dotenv import load_dotenv
from pymongo import MongoClient, errors

load_dotenv()

logger = logging.getLogger("MongoService")
logging.basicConfig(level=logging.INFO)


class MongoService:
    instance = None

    def __new__(cls, mongo_url=None):
        if cls.instance:
            return cls.instance
        obj = super().__new__(cls)
        cls.instance = obj
        return obj

    def __init__(self, mongo_url=None):
        if getattr(self, "_initialized", False):
            return

        self.mongo_url = mongo_url or os.getenv("MONGO_URL")
        if not self.mongo_url:
            raise ValueError("Missing MONGO_URL in environment")

        self.client = None
        self.connected = False
        self._initialized = True

    def is_connected(self):
        """Checks actual Mongo health, not just stored flag."""
        if not self.client:
            return False
        try:
            self.client.admin.command("ping")
            return True
        except Exception:
            return False

    def ensure_connection(self):
        """Reconnect if ping fails."""
        if not self.is_connected():
            logger.warning("Mongo connection lost. Reconnecting...")
            try:
                self.connect()
            except Exception as e:
                logger.error(f"Reconnect failed: {e}")
                raise

    def connect(self):
        """Connect to MongoDB once, fail fast if not possible."""
        try:
            self.client = MongoClient(
                self.mongo_url,
                maxPoolSize=10,
                minPoolSize=2,
                serverSelectionTimeoutMS=5000,
                socketTimeoutMS=45000,
            )
            self.client.admin.command("ping")
            self.connected = True
            logger.info("Connected to MongoDB")
        except errors.PyMongoError as e:
            self.connected = False
            logger.error(f"MongoDB connection failed: {e}")
            raise

    def reconnect_once(self):
        """Try reconnecting once if disconnected."""
        if self.connected:
            return True
        logger.warning("MongoDB client not connected. Retrying once...")
        try:
            self.connect()
            return True
        except Exception as e:
            logger.error(f"MongoDB reconnect failed: {e}")
            self.connected = False
            return False

    def db(self, name: str = os.environ["MONGO_DATABASE"]):
        self.ensure_connection()
        return self.client.get_database(name)
