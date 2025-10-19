from datetime import timedelta

from temporalio import workflow

from .transformation import TransformationWorkflow


@workflow.defn
class MainWorkflow:
    @workflow.run
    async def run(self, input: dict):
        info = workflow.info()
        pipeline_config = await workflow.execute_activity(
            "fetch_pipeline_config",
            input.get("metadata", {}).get("pipeline_id", ""),
            schedule_to_close_timeout=timedelta(minutes=5),
        )
        log_data = input.get("log_data", {})
        start_node_id = input.get("metadata", {}).get("start_node_id")
        self.node_outputs = {"input": log_data}

        print("TRIGGERING TASK Q:", info.task_queue + "-transform", info.workflow_id)

        return await workflow.execute_child_workflow(
            TransformationWorkflow,
            args=(pipeline_config, log_data, start_node_id),
            task_queue=info.task_queue + "-transform",
            id=info.workflow_id + "-transform",
        )
