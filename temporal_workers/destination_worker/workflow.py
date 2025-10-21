from datetime import timedelta
from typing import Any

from temporalio import workflow


@workflow.defn
class DestinationWorkflow:
    @workflow.run
    async def run(self, input: Any, target_id: str, metadata: dict):
        org_id = metadata.get("organisationId")
        project_id = metadata.get("projectId")
        pipeline_id = metadata.get("pipelineId")

        if not (org_id and project_id and pipeline_id):
            return {
                "successs": False,
                "reason": "organisationId, projectId and pipelineId required",
                "context": {"metadata": metadata},
                "log_data": input,
            }

        target = await workflow.execute_activity(
            "fetch_integration_target",
            args=(org_id, project_id, pipeline_id, target_id),
            schedule_to_close_timeout=timedelta(minutes=5),
        )

        if not target or "connectorId" not in target:
            return {
                "success": False,
                "reason": "Required Target info not found",
                "context": {"metadata": metadata, "pipeline_config": target},
                "log_data": input,
            }

        connector = await workflow.execute_activity(
            "fetch_integration_connector",
            args=(org_id, project_id, target["connectorId"]),
            schedule_to_close_timeout=timedelta(minutes=5),
        )

        execution = await workflow.execute_activity(
            "send_to_destination",
            args=(input, target, connector),
            schedule_to_close_timeout=timedelta(minutes=5),
        )

        print("EXECUTION DONE: ", execution)

        return {"success": True}
