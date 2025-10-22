"""
Workflow-safe utility functions.

These functions can be imported in workflows without triggering sandbox restrictions.
"""


def extract_exception_details(exc: Exception) -> tuple[str, str, str]:
    """
    Extract exception details as JSON-serializable strings.

    Safe to use in workflows - no non-deterministic imports.
    Unwraps Temporal ActivityError/ApplicationError to get root cause.

    Args:
        exc: The exception to extract details from

    Returns:
        Tuple of (exc_type, exc_message, exc_stack)
    """
    # Collect full chain of exceptions for stack trace
    exception_chain = []
    current_exc = exc

    # Build exception chain
    while current_exc is not None:
        exception_chain.append(current_exc)
        current_exc = getattr(current_exc, "cause", None)

    # Get the root cause (last in chain)
    root_cause = exception_chain[-1] if exception_chain else exc

    exc_type = type(root_cause).__name__
    exc_message = str(root_cause)

    # Build stack trace from exception chain
    stack_parts = []

    for i, ex in enumerate(exception_chain):
        is_root = i == len(exception_chain) - 1

        # Add exception info
        ex_type = type(ex).__name__
        ex_msg = str(ex)

        if is_root:
            # For root cause, try to get detailed stack
            if hasattr(ex, "__traceback_str__"):
                stack_parts.append(ex.__traceback_str__)
            else:
                stack_parts.append(f"{ex_type}: {ex_msg}")
        else:
            # For wrapper exceptions, just note the wrapping
            if ex_type in ["ActivityError", "ApplicationError"]:
                # Skip generic wrappers if they don't add info
                if ex_msg and ex_msg != "Activity task failed":
                    stack_parts.append(f"[{ex_type}] {ex_msg}")
            else:
                stack_parts.append(f"[{ex_type}] {ex_msg}")

    exc_stack = "\n".join(stack_parts) if stack_parts else f"{exc_type}: {exc_message}"

    return exc_type, exc_message, exc_stack
