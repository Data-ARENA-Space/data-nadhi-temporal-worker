import json

from temporalio import activity


@activity.defn
async def fetch_pipeline_config(pipeline_id: str) -> dict:
    print(pipeline_id)
    with open("test-workflow.json") as f:
        pipeline_config = json.load(f)
    print("Pipeline config fetched!")
    return pipeline_config
