import importlib


class DestinationRegistry:
    _registry = {
        "slack-bot": "temporal_workers.destination_worker.destinations.slack.SlackDestination",
    }

    @classmethod
    def get(cls, name: str):
        path = cls._registry.get(name)
        if not path:
            raise ValueError(f"Unknown destination: {name}")

        module_name, class_name = path.rsplit(".", 1)
        module = importlib.import_module(module_name)
        return getattr(module, class_name)
