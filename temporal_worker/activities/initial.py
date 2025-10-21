from temporalio import activity

from ..utils.dal.pipeline import get_pipeline, get_workflow_config


@activity.defn
async def fetch_pipeline_config(org_id: str, project_id: str, pipeline_id: str) -> dict:
    return get_pipeline(org_id, project_id, pipeline_id)


@activity.defn
async def fetch_workflow_config(org_id: str, project_id: str, pipeline_id: str) -> dict:
    return get_workflow_config(org_id, project_id, pipeline_id)
