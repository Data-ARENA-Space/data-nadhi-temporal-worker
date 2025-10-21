from abc import ABC, abstractmethod
from typing import Any


class Destination(ABC):
    def __init__(self, input: Any, target_config: dict, connector_config: dict):
        self.input = input
        self.target_config = target_config
        self.connector_config = connector_config

    @abstractmethod
    def send(self):
        pass
