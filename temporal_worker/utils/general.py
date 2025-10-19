import json
from typing import Any


class PipelineUtils:
    @staticmethod
    def fetch_pipeline_config(pipeline_id: str) -> dict:
        print(pipeline_id)
        with open("workflow.json") as f:
            pipeline_config = json.load(f)
        print("Pipeline config fetched!")
        return pipeline_config

    @staticmethod
    def get_keychain_from_str(keychain_str: str) -> Any:
        return keychain_str[2:].split(".")

    @staticmethod
    def _is_keychain_str(keychain_str: str) -> bool:
        return type(keychain_str) is str and keychain_str.startswith("$.")
