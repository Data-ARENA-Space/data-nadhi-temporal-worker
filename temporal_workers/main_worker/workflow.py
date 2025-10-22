from datetime import timedelta

from temporalio import workflow
from temporalio.exceptions import ActivityError, ApplicationError

from utils.retry_policies import get_default_retry_policy
from utils.workflow_utils import extract_exception_details


@workflow.defn
class MainWorkflow:
    @workflow.run
    async def run(self, input: dict):
        info = workflow.info()

        metadata = input.get("metadata", {})
        log_data = input.get("log_data", {})

        org_id = metadata.get("organisationId")
        project_id = metadata.get("projectId")
        pipeline_id = metadata.get("pipelineId")
        message_id = metadata.get("messageId")

        # Build context for logging in activities
        ctx = {
            "organisationId": org_id,
            "projectId": project_id,
            "pipelineId": pipeline_id,
            "messageId": message_id,
            "logData": log_data,
            "originalInput": log_data,
        }

        if not (org_id and project_id and pipeline_id):
            return {
                "success": False,
                "reason": "organisationId, projectId and pipelineId required",
                "context": {"metadata": metadata},
            }

        try:
            pipeline_config = await workflow.execute_activity(
                "fetch_pipeline_config",
                args=(org_id, project_id, pipeline_id, ctx),
                schedule_to_close_timeout=timedelta(minutes=5),
                retry_policy=get_default_retry_policy(),
            )
        except (ApplicationError, ActivityError) as exc:
            # Log failure and return error response
            exc_type, exc_message, exc_stack = extract_exception_details(exc)
            return await workflow.execute_activity(
                "log_failure",
                args=(exc_type, exc_message, exc_stack, exc_message, ctx, None, None),
                schedule_to_close_timeout=timedelta(minutes=2),
            )

        if not pipeline_config or "startNodeId" not in pipeline_config:
            return {
                "success": False,
                "reason": "Start Node Id not found",
                "context": {"pipeline_config": pipeline_config},
            }

        try:
            workflow_config = await workflow.execute_activity(
                "fetch_workflow_config",
                args=(org_id, project_id, pipeline_id, ctx),
                schedule_to_close_timeout=timedelta(minutes=5),
                retry_policy=get_default_retry_policy(),
            )
        except (ApplicationError, ActivityError) as exc:
            # Log failure and return error response
            exc_type, exc_message, exc_stack = extract_exception_details(exc)
            return await workflow.execute_activity(
                "log_failure",
                args=(exc_type, exc_message, exc_stack, exc_message, ctx, None, None),
                schedule_to_close_timeout=timedelta(minutes=2),
            )

        start_node_id = pipeline_config["startNodeId"]
        self.node_outputs = {"input": log_data}

        return await workflow.execute_child_workflow(
            "TransformationWorkflow",
            args=(workflow_config, log_data, start_node_id, ctx),
            task_queue=info.task_queue + "-transform",
            id=info.workflow_id + "-transform",
        )
