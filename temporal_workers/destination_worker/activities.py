from typing import Any

from temporalio import activity

from utils.exceptions import IntegrationNotFoundError, UnsupportedIntegrationTypeError
from utils.logger import log_debug, log_error

from .dal import get_integration_connector, get_integration_target
from .destinations import DestinationRegistry
from .destinations.core import Destination


@activity.defn
async def fetch_integration_connector(
    org_id: str, project_id: str, connector_id: str, ctx: dict
) -> dict:
    log_debug(
        "Fetching integration connector from DB",
        ctx,
        {"connector_id": connector_id},
    )
    connector = get_integration_connector(org_id, project_id, connector_id)
    if not connector:
        log_error(
            "Integration connector not found", ctx, {"connector_id": connector_id}
        )
        raise IntegrationNotFoundError(
            f"Integration connector not found: {connector_id}"
        )
    return connector


@activity.defn
async def fetch_integration_target(
    org_id: str, project_id: str, pipeline_id: str, target_id: str, ctx: dict
) -> dict:
    log_debug("Fetching integration target from DB", ctx, {"target_id": target_id})
    target = get_integration_target(org_id, project_id, pipeline_id, target_id)
    if not target:
        log_error("Integration target not found", ctx, {"target_id": target_id})
        raise IntegrationNotFoundError(f"Integration target not found: {target_id}")
    return target


@activity.defn
async def send_to_destination(input: Any, target: dict, connector: dict, ctx: dict):
    log_debug(
        "Sending to destination",
        ctx,
        {"integration_type": connector.get("integrationType")},
    )
    destination_class = DestinationRegistry.get(connector["integrationType"])
    if not destination_class:
        log_error(
            "Unsupported integration type",
            ctx,
            {"integration_type": connector["integrationType"]},
        )
        raise UnsupportedIntegrationTypeError(
            f"Unsupported integration type: {connector['integrationType']}"
        )

    try:
        destination: Destination = destination_class(input, target, connector)
        result = destination.send()
        log_debug("Destination send completed", ctx, {"result": result})
        return result
    except (ValueError, KeyError, TypeError) as exc:
        # Data/config errors - non-retryable
        raise UnsupportedIntegrationTypeError(
            f"Destination configuration error: {str(exc)}"
        ) from exc
    except Exception:
        # Other exceptions (connection issues, etc.) are retryable
        raise
