import os

from dotenv import load_dotenv
from pymongo import MongoClient, errors

from utils.logger import log_debug, log_error, log_warn

load_dotenv()


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
            log_warn("Mongo connection lost, reconnecting", None, {"component": "MongoService"})
            try:
                self.connect()
            except Exception as e:
                log_error("Reconnect failed", None, {"component": "MongoService", "error": str(e)})
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
            log_debug("Connected to MongoDB", None, {"component": "MongoService"})
        except errors.PyMongoError as e:
            self.connected = False
            log_error("MongoDB connection failed", None, {"component": "MongoService", "error": str(e)})
            raise

    def reconnect_once(self):
        """Try reconnecting once if disconnected."""
        if self.connected:
            return True
        log_warn("MongoDB client not connected, retrying once", None, {"component": "MongoService"})
        try:
            self.connect()
            return True
        except Exception as e:
            log_error("MongoDB reconnect failed", None, {"component": "MongoService", "error": str(e)})
            self.connected = False
            return False

    def db(self, name: str = os.environ["MONGO_DATABASE"]):
        self.ensure_connection()
        return self.client.get_database(name)
