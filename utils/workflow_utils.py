"""
Workflow-safe utility functions.

These functions can be imported in workflows without triggering sandbox restrictions.
"""
from typing import Tuple


def extract_exception_details(exc: Exception) -> Tuple[str, str, str]:
    """
    Extract exception details as JSON-serializable strings.
    
    Safe to use in workflows - no non-deterministic imports.
    
    Args:
        exc: The exception to extract details from
        
    Returns:
        Tuple of (exc_type, exc_message, exc_stack)
    """
    exc_type = type(exc).__name__
    exc_message = str(exc)
    exc_stack = getattr(exc, '__traceback_str__', str(exc))
    return exc_type, exc_message, exc_stack
