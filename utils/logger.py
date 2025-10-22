import json
import sys
import traceback
from datetime import datetime
from typing import Any


def _prune_nullish(obj: dict[str, Any]) -> dict[str, Any]:
    """Remove keys with None or empty values."""
    return {k: v for k, v in (obj or {}).items() if v is not None and v != ""}


def _build_log_entry(
    level: str,
    message: str,
    ctx: dict[str, Any] | None = None,
    extra: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Build a structured log entry with top-level meta fields."""
    ctx = ctx or {}
    extra = extra or {}

    entry = {
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "level": level,
        "message": message,
    }

    # Extract top-level fields from ctx
    if ctx.get("organisationId"):
        entry["organisationId"] = ctx["organisationId"]
    if ctx.get("projectId"):
        entry["projectId"] = ctx["projectId"]
    if ctx.get("pipelineId"):
        entry["pipelineId"] = ctx["pipelineId"]
    if ctx.get("traceId"):
        entry["traceId"] = ctx["traceId"]
    if ctx.get("logData"):
        entry["logData"] = ctx["logData"]

    # Add extra fields to context
    context = {
        k: v
        for k, v in ctx.items()
        if k not in {"organisationId", "projectId", "pipelineId", "traceId", "logData"}
    }
    context.update(extra)

    if context:
        entry["context"] = _prune_nullish(context)

    return entry


def _emit(entry: dict[str, Any]):
    """Emit JSON log to stdout."""
    try:
        print(json.dumps(entry), file=sys.stdout, flush=True)
    except Exception:
        # Fallback for circular references
        print(
            json.dumps({**entry, "context": str(entry.get("context"))}),
            file=sys.stdout,
            flush=True,
        )


def log_debug(
    message: str, ctx: dict[str, Any] | None = None, extra: dict[str, Any] | None = None
):
    """Log a debug message."""
    _emit(_build_log_entry("debug", message, ctx, extra))


def log_info(
    message: str, ctx: dict[str, Any] | None = None, extra: dict[str, Any] | None = None
):
    """Log an info message."""
    _emit(_build_log_entry("info", message, ctx, extra))


def log_warn(
    message: str, ctx: dict[str, Any] | None = None, extra: dict[str, Any] | None = None
):
    """Log a warning message."""
    _emit(_build_log_entry("warn", message, ctx, extra))


def log_error(
    message: str, ctx: dict[str, Any] | None = None, extra: dict[str, Any] | None = None
):
    """Log an error message."""
    _emit(_build_log_entry("error", message, ctx, extra))


def log_exception(
    exc: Exception,
    message: str,
    ctx: dict[str, Any] | None = None,
    extra: dict[str, Any] | None = None,
):
    """Log an exception with stack trace."""
    extra = extra or {}
    extra["exception"] = {
        "type": type(exc).__name__,
        "message": str(exc),
        "stack": traceback.format_exc(),
    }
    _emit(_build_log_entry("error", message, ctx, extra))


def log_success(
    message: str, ctx: dict[str, Any] | None = None, extra: dict[str, Any] | None = None
):
    """Log a success info message (alias for log_info)."""
    log_info(message, ctx, extra)
