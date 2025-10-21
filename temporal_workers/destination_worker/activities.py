from typing import Any

from temporalio import activity

from .dal import get_integration_connector, get_integration_target
from .destinations import DestinationRegistry
from .destinations.core import Destination


@activity.defn
async def fetch_integration_connector(
    org_id: str, project_id: str, connector_id: str
) -> dict:
    return get_integration_connector(org_id, project_id, connector_id)


@activity.defn
async def fetch_integration_target(
    org_id: str, project_id: str, pipeline_id: str, target_id: str
) -> dict:
    return get_integration_target(org_id, project_id, pipeline_id, target_id)


@activity.defn
async def send_to_destination(input: Any, target: dict, connector: dict):
    destination_class = DestinationRegistry.get(connector["integrationType"])
    if not destination_class:
        return {
            "success": False,
            "reason": "Unsupported integration type",
            "context": {
                "integration_type": connector["integrationType"],
            },
            "log_data": input,
        }

    destination: Destination = destination_class(input, target, connector)
    return destination.send()
