from temporalio import activity

from utils.exceptions import PipelineNotFoundError, WorkflowConfigNotFoundError
from utils.logger import log_debug

from .dal import get_pipeline, get_workflow_config


@activity.defn
async def fetch_pipeline_config(
    org_id: str, project_id: str, pipeline_id: str, ctx: dict
) -> dict:
    log_debug("Fetching pipeline config from DB", ctx)
    pipeline = get_pipeline(org_id, project_id, pipeline_id)
    if not pipeline:
        raise PipelineNotFoundError(f"Pipeline not found: {pipeline_id}")
    return pipeline


@activity.defn
async def fetch_workflow_config(
    org_id: str, project_id: str, pipeline_id: str, ctx: dict
) -> dict:
    log_debug("Fetching workflow config from DB", ctx)
    workflow_config = get_workflow_config(org_id, project_id, pipeline_id)
    if not workflow_config:
        raise WorkflowConfigNotFoundError(
            f"Workflow config not found for pipeline: {pipeline_id}"
        )
    return workflow_config
