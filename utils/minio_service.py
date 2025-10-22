import json
import os
from io import BytesIO

from minio import Minio


class MinioService:
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._initialized = False
        return cls._instance

    def __init__(self):
        if self._initialized:
            return

        self.endpoint = os.getenv("MINIO_ENDPOINT", "datanadhi-minio:9000")
        self.access_key = os.getenv("MINIO_ACCESS_KEY", "minio")
        self.secret_key = os.getenv("MINIO_SECRET_KEY", "minio123")
        self.bucket_name = os.getenv("MINIO_BUCKET", "failure-logs")
        self.secure = os.getenv("MINIO_SECURE", "false").lower() == "true"

        self.client = Minio(
            self.endpoint,
            access_key=self.access_key,
            secret_key=self.secret_key,
            secure=self.secure,
        )

        # Ensure bucket exists
        try:
            if not self.client.bucket_exists(self.bucket_name):
                self.client.make_bucket(self.bucket_name)
        except Exception as e:
            print(f"Warning: Could not create bucket {self.bucket_name}: {e}")

        self._initialized = True

    def upload_json(self, object_path: str, data: dict) -> bool:
        """
        Upload JSON data to MinIO.

        Args:
            object_path: Path within bucket (e.g., "org/project/pipeline/message.json")
            data: Dictionary to upload as JSON

        Returns:
            True if successful, False otherwise
        """
        try:
            json_data = json.dumps(data, indent=2)
            json_bytes = json_data.encode("utf-8")
            json_stream = BytesIO(json_bytes)

            self.client.put_object(
                self.bucket_name,
                object_path,
                json_stream,
                length=len(json_bytes),
                content_type="application/json",
            )
            return True
        except Exception as e:
            print(f"Failed to upload to MinIO: {e}")
            return False
