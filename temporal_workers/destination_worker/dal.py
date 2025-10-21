import json
import os

from dotenv import load_dotenv

from utils.db.mongo import MongoService
from utils.db.redis import RedisService

from .utils import decrypt_aes_gcm

load_dotenv()


def get_integration_target(org_id, project_id, pipeline_id, target_id):
    mongo = MongoService()
    redis = RedisService()

    # Fetch from cache
    integration_target = redis.safe_get(
        f"datanadhiserver:org:{org_id}:prj:{project_id}:pl:{pipeline_id}:it:{target_id}"
    )

    if integration_target:
        return json.loads(integration_target)

    integration_targets = mongo.db().get_collection("IntegrationTargets")
    integration_target = integration_targets.find_one(
        {
            "organisationId": org_id,
            "projectId": project_id,
            "pipelineId": pipeline_id,
            "targetId": target_id,
        }
    )

    del integration_target["_id"]

    redis.safe_set(
        f"datanadhiserver:org:{org_id}:prj:{project_id}:pl:{pipeline_id}:it:{target_id}",
        json.dumps(integration_target),
        ex=3600,
    )

    return integration_target


def get_integration_connector(org_id, project_id, connector_id):
    mongo = MongoService()
    redis = RedisService()

    # Fetch from cache
    integration_connector = redis.safe_get(
        f"datanadhiserver:org:{org_id}:prj:{project_id}:ic:{connector_id}:decrypted"
    )

    if integration_connector:
        return json.loads(integration_connector)

    integration_connectors = mongo.db().get_collection("IntegrationConnectors")
    integration_connector = integration_connectors.find_one(
        {
            "organisationId": org_id,
            "projectId": project_id,
            "connectorId": connector_id,
        }
    )

    del integration_connector["_id"]

    encrypted_creds = integration_connector["encryptedCredentials"]
    decrypted_creds = decrypt_aes_gcm(encrypted_creds, os.environ["SEC_DB"])

    integration_connector["creds"] = json.loads(decrypted_creds)

    redis.safe_set(
        f"datanadhiserver:org:{org_id}:prj:{project_id}:ic:{connector_id}:decrypted",
        json.dumps(integration_connector),
        ex=3600,
    )

    return integration_connector
