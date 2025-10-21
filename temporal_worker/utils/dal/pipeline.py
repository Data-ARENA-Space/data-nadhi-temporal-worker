import json

from ..db.mongo import MongoService
from ..db.redis import RedisService


def get_workflow_config(org_id: str, project_id: str, pipeline_id: str):
    mongo = MongoService()
    redis = RedisService()

    # Fetch from cache
    workflow_config = redis.safe_get(
        f"datanadhiserver:org:{org_id}:prj:{project_id}:pl:{pipeline_id}:workflow"
    )

    if workflow_config:
        return json.loads(workflow_config)

    pipeline_nodes = mongo.db().get_collection("PipelineNodes")
    nodes = pipeline_nodes.find(
        {"organisationId": org_id, "projectId": project_id, "pipelineId": pipeline_id}
    )

    workflow_config = {}

    for node in nodes:
        workflow_config[node["nodeId"]] = node["nodeConfig"]

    redis.safe_set(
        f"datanadhiserver:org:{org_id}:prj:{project_id}:pl:{pipeline_id}:workflow",
        json.dumps(workflow_config),
        ex=3600,
    )

    return workflow_config


def get_pipeline(org_id: str, project_id: str, pipeline_id: str):
    mongo = MongoService()
    redis = RedisService()

    # Fetch from cache
    pipeline = redis.safe_get(
        f"datanadhiserver:org:{org_id}:prj:{project_id}:pl:{pipeline_id}"
    )

    if pipeline:
        return json.loads(pipeline)

    pipelines = mongo.db().get_collection("Pipelines")
    pipeline = pipelines.find_one(
        {"organisationId": org_id, "projectId": project_id, "pipelineId": pipeline_id}
    )

    del pipeline["_id"]

    redis.safe_set(
        f"datanadhiserver:org:{org_id}:prj:{project_id}:pl:{pipeline_id}",
        json.dumps(pipeline),
        ex=3600,
    )

    return pipeline
