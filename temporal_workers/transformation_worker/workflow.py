import copy
from datetime import timedelta

from temporalio import workflow
from temporalio.exceptions import ActivityError, ApplicationError

from utils.retry_policies import get_default_retry_policy
from utils.workflow_utils import extract_exception_details


@workflow.defn
class TransformationWorkflow:
    def __init__(self):
        self.queue = []
        self.node_outputs = {}

    @workflow.run
    async def traverse_workflow(
        self, pipeline_config: dict, log_data: dict, start_node_id: str, ctx: dict
    ) -> dict:
        info = workflow.info()
        if start_node_id not in pipeline_config:
            return {
                "success": False,
                "reason": "Start node not found in pipeline config",
            }

        self.queue.append({"node_id": start_node_id, "data": copy.deepcopy(log_data)})

        while self.queue:
            current_input = self.queue.pop(0)
            current_node_id = current_input.get("node_id")
            current_data = current_input.get("data")

            current_node_config = pipeline_config.get(current_node_id)

            node_type = current_node_config.get("type")

            next_nodes, final_data = [], current_data
            if node_type == "transformation":
                try:
                    result = await workflow.execute_activity(
                        "transform",
                        args=(current_node_config, current_data, ctx),
                        schedule_to_close_timeout=timedelta(minutes=5),
                        retry_policy=get_default_retry_policy(),
                    )
                    next_nodes, final_data = result
                except (ApplicationError, ActivityError) as exc:
                    # Log failure and return error response
                    exc_type, exc_message, exc_stack = extract_exception_details(exc)
                    final_data = await workflow.execute_activity(
                        "log_failure",
                        args=(
                            exc_type,
                            exc_message,
                            exc_stack,
                            exc_message,
                            ctx,
                            current_data,
                            None,
                        ),
                        schedule_to_close_timeout=timedelta(minutes=2),
                    )
            elif node_type == "condition-branching":
                try:
                    result = await workflow.execute_activity(
                        "filters",
                        args=(current_node_config, current_data, ctx),
                        schedule_to_close_timeout=timedelta(minutes=5),
                        retry_policy=get_default_retry_policy(),
                    )
                    next_nodes, final_data = result
                except (ApplicationError, ActivityError) as exc:
                    # Log failure and return error response
                    exc_type, exc_message, exc_stack = extract_exception_details(exc)
                    final_data = await workflow.execute_activity(
                        "log_failure",
                        args=(
                            exc_type,
                            exc_message,
                            exc_stack,
                            exc_message,
                            ctx,
                            current_data,
                            None,
                        ),
                        schedule_to_close_timeout=timedelta(minutes=2),
                    )
            elif node_type == "end":
                final_data = await workflow.execute_child_workflow(
                    "DestinationWorkflow",
                    args=(current_data, current_node_config["target_id"], ctx),
                    task_queue=info.task_queue.replace("-transform", "-destination"),
                    id=info.workflow_id.replace("-transform", "-destination"),
                )

            for nd in next_nodes:
                self.queue.append({"node_id": nd, "data": final_data})

            self.node_outputs[current_node_id] = final_data

        return {"success": True, "node_outputs": self.node_outputs}
