import traceback
from datetime import datetime
from typing import Any

from temporalio import activity

from utils.logger import log_error
from utils.minio_service import MinioService


def prepare_failure_data(
    exc: Exception,
    message: str,
    ctx: dict[str, Any],
    current_input: Any = None,
    extra_context: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """
    Prepare failure data for logging.

    Args:
        exc: The exception that occurred
        message: Human-readable error message
        ctx: Context dict containing organisationId, projectId, pipelineId,
            messageId, originalInput
        current_input: The current input data being processed
            (if different from original)
        extra_context: Additional context to include in the failure log

    Returns:
        Failure data dict ready for MinIO upload
    """
    org_id = ctx.get("organisationId")
    project_id = ctx.get("projectId")
    pipeline_id = ctx.get("pipelineId")
    message_id = ctx.get("messageId")
    original_input = ctx.get("originalInput") or ctx.get("logData")

    # Build failure data
    failure_data = {
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "organisationId": org_id,
        "projectId": project_id,
        "pipelineId": pipeline_id,
        "messageId": message_id,
        "originalInput": original_input,
        "currentInput": current_input if current_input != original_input else None,
        "error": {
            "message": str(exc),
            "type": type(exc).__name__,
            "stack": traceback.format_exc()
            if traceback.format_exc() != "NoneType: None\n"
            else str(exc),
            "description": message,
        },
    }

    # Add extra context if provided
    if extra_context:
        failure_data["context"] = extra_context

    return failure_data


@activity.defn
async def log_failure(
    exc_type: str,
    exc_message: str,
    exc_stack: str,
    description: str,
    ctx: dict[str, Any],
    current_input: Any = None,
    extra_context: dict[str, Any] | None = None
) -> dict[str, Any]:
    """
    Activity to log failure data to MinIO and return failure response.

    Args:
        exc_type: The exception type name as string
        exc_message: The exception message as string
        exc_stack: The exception stack trace as string
        description: Human-readable error description
        ctx: Context dict containing organisationId, projectId, pipelineId, messageId
        current_input: The current input data being processed (if different from original)
        extra_context: Additional context to include in the failure log

    Returns:
        Failure response dict with success=False, reason, and error details
    """
    org_id = ctx.get("organisationId")
    project_id = ctx.get("projectId")
    pipeline_id = ctx.get("pipelineId")
    message_id = ctx.get("messageId")
    original_input = ctx.get("originalInput") or ctx.get("logData")
    
    # Build failure data manually with the stack trace we received
    failure_data = {
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "organisationId": org_id,
        "projectId": project_id,
        "pipelineId": pipeline_id,
        "messageId": message_id,
        "originalInput": original_input,
        "currentInput": current_input if current_input != original_input else None,
        "error": {
            "message": exc_message,
            "type": exc_type,
            "stack": exc_stack,
            "description": description,
        },
    }
    
    if extra_context:
        failure_data["context"] = extra_context

    org_id = failure_data.get("organisationId")
    project_id = failure_data.get("projectId")
    pipeline_id = failure_data.get("pipelineId")
    message_id = failure_data.get("messageId")

    # Attempt to log to MinIO (best effort)
    if org_id and project_id and pipeline_id and message_id:
        try:
            minio = MinioService()
            object_path = f"{org_id}/{project_id}/{pipeline_id}/{message_id}.json"
            success = minio.upload_json(object_path, failure_data)

            if success:
                log_error("Failure logged to MinIO", ctx, {"minio_path": object_path})
            else:
                log_error("Failed to log to MinIO", ctx)
        except Exception as minio_exc:
            log_error("MinIO logging error", ctx, {"minio_error": str(minio_exc)})
    else:
        log_error("Missing required IDs for MinIO logging", ctx)

    # Return failure response for workflow
    error_info = failure_data.get("error", {})
    return {
        "success": False,
        "reason": error_info.get("description", "Unknown error"),
        "error": {
            "type": error_info.get("type", "Error"),
            "message": error_info.get("message", ""),
        },
    }
