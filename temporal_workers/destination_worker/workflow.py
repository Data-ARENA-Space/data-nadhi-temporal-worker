from datetime import timedelta
from typing import Any

from temporalio import workflow
from temporalio.exceptions import ActivityError, ApplicationError

from utils.retry_policies import get_default_retry_policy
from utils.workflow_utils import extract_exception_details


@workflow.defn
class DestinationWorkflow:
    @workflow.run
    async def run(self, input: Any, target_id: str, ctx: dict):
        org_id = ctx.get("organisationId")
        project_id = ctx.get("projectId")
        pipeline_id = ctx.get("pipelineId")

        if not (org_id and project_id and pipeline_id):
            return {
                "success": False,
                "reason": "organisationId, projectId and pipelineId required",
            }

        try:
            target = await workflow.execute_activity(
                "fetch_integration_target",
                args=(org_id, project_id, pipeline_id, target_id, ctx),
                schedule_to_close_timeout=timedelta(minutes=5),
                retry_policy=get_default_retry_policy(),
            )
        except (ApplicationError, ActivityError) as exc:
            # Log failure and return error response
            exc_type, exc_message, exc_stack = extract_exception_details(exc)
            return await workflow.execute_activity(
                "log_failure",
                args=(
                    exc_type,
                    exc_message,
                    exc_stack,
                    exc_message,
                    ctx,
                    input,
                    None,
                    "DestinationWorkflow-fetch_integration_target",
                ),
                schedule_to_close_timeout=timedelta(minutes=2),
            )

        if not target or "connectorId" not in target:
            return {
                "success": False,
                "reason": "Required Target info not found",
            }

        try:
            connector = await workflow.execute_activity(
                "fetch_integration_connector",
                args=(org_id, project_id, target["connectorId"], ctx),
                schedule_to_close_timeout=timedelta(minutes=5),
                retry_policy=get_default_retry_policy(),
            )
        except (ApplicationError, ActivityError) as exc:
            # Log failure and return error response
            exc_type, exc_message, exc_stack = extract_exception_details(exc)
            return await workflow.execute_activity(
                "log_failure",
                args=(
                    exc_type,
                    exc_message,
                    exc_stack,
                    exc_message,
                    ctx,
                    input,
                    None,
                    "DestinationWorkflow-fetch_integration_connector",
                ),
                schedule_to_close_timeout=timedelta(minutes=2),
            )

        try:
            execution = await workflow.execute_activity(
                "send_to_destination",
                args=(input, target, connector, ctx),
                schedule_to_close_timeout=timedelta(minutes=5),
                retry_policy=get_default_retry_policy(),
            )
        except (ApplicationError, ActivityError) as exc:
            # Log failure and return error response
            exc_type, exc_message, exc_stack = extract_exception_details(exc)
            return await workflow.execute_activity(
                "log_failure",
                args=(
                    exc_type,
                    exc_message,
                    exc_stack,
                    exc_message,
                    ctx,
                    input,
                    None,
                    "DestinationWorkflow-send_to_destination",
                ),
                schedule_to_close_timeout=timedelta(minutes=2),
            )

        return {"success": True, "execution": execution}
