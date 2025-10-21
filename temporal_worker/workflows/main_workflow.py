from datetime import timedelta

from temporalio import workflow

from .transformation import TransformationWorkflow


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

        if not (org_id and project_id and pipeline_id):
            return {
                "successs": False,
                "reason": "organisationId, projectId and pipelineId required",
                "context": {"metadata": metadata},
                "log_data": log_data,
            }
        pipeline_config = await workflow.execute_activity(
            "fetch_pipeline_config",
            args=(org_id, project_id, pipeline_id),
            schedule_to_close_timeout=timedelta(minutes=5),
        )

        if not pipeline_config or "startNodeId" not in pipeline_config:
            return {
                "success": False,
                "reason": "Start Node Id not found",
                "context": {"metadata": metadata, "pipeline_config": pipeline_config},
                "log_data": log_data,
            }

        workflow_config = await workflow.execute_activity(
            "fetch_workflow_config",
            args=(org_id, project_id, pipeline_id),
            schedule_to_close_timeout=timedelta(minutes=5),
        )

        start_node_id = pipeline_config["startNodeId"]
        self.node_outputs = {"input": log_data}

        print("TRIGGERING TASK Q:", info.task_queue + "-transform", info.workflow_id)

        return await workflow.execute_child_workflow(
            TransformationWorkflow,
            args=(workflow_config, log_data, start_node_id),
            task_queue=info.task_queue + "-transform",
            id=info.workflow_id + "-transform",
        )
