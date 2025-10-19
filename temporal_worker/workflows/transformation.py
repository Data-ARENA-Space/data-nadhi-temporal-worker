import copy
from datetime import timedelta

from temporalio import workflow

from ..activities.transformations import end, filters, transform


@workflow.defn
class TransformationWorkflow:
    def __init__(self):
        self.queue = []
        self.type_to_function = {
            "transformation": transform,
            "condition-branching": filters,
            "end": end,
        }
        self.node_outputs = {}

    @workflow.run
    async def traverse_workflow(
        self, pipeline_config: dict, log_data: dict, start_node_id: str
    ) -> dict:
        if start_node_id not in pipeline_config:
            print(start_node_id, pipeline_config)
            return {"error": "Start node not found in pipeline config"}
        self.queue.append({"node_id": start_node_id, "data": copy.deepcopy(log_data)})

        while self.queue:
            current_input = self.queue.pop(0)
            current_node_id = current_input.get("node_id")
            current_data = current_input.get("data")

            current_node_config = pipeline_config.get(current_node_id)

            node_type = current_node_config.get("type")

            if node_type == "transformation":
                next_nodes, final_data = await workflow.execute_activity(
                    "transform",
                    args=(current_node_config, current_data),
                    schedule_to_close_timeout=timedelta(minutes=5),
                )
            elif node_type == "condition-branching":
                next_nodes, final_data = await workflow.execute_activity(
                    "filters",
                    args=(current_node_config, current_data),
                    schedule_to_close_timeout=timedelta(minutes=5),
                )
            elif node_type == "end":
                next_nodes, final_data = await workflow.execute_activity(
                    "end",
                    args=(current_node_config, current_data),
                    schedule_to_close_timeout=timedelta(minutes=5),
                )
            else:
                next_nodes, final_data = [], current_data

            for nd in next_nodes:
                self.queue.append({"node_id": nd, "data": final_data})

            self.node_outputs[current_node_id] = final_data

        print("TRANSFORMATION COMPLETE :", self.node_outputs)

        return self.node_outputs
