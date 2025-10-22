"""
Retry policy configurations for Temporal activities.
"""

from datetime import timedelta

from temporalio.common import RetryPolicy

# Non-retryable exception types (business logic errors)
NON_RETRYABLE_EXCEPTIONS = [
    # Our custom exceptions
    "DataNadhiError",
    "PipelineNotFoundError",
    "WorkflowConfigNotFoundError",
    "InvalidPipelineConfigError",
    "TransformationError",
    "FilterEvaluationError",
    "InvalidTransformationError",
    "IntegrationNotFoundError",
    "UnsupportedIntegrationTypeError",
    "DestinationSendError",
    "InvalidInputDataError",
    "MissingRequiredFieldError",
    # Python built-in exceptions that indicate programming/data errors
    "ValueError",
    "KeyError",
    "TypeError",
    "AttributeError",
    "IndexError",
    "ZeroDivisionError",
    "AssertionError",
]


def get_default_retry_policy() -> RetryPolicy:
    """
    Default retry policy for activities.

    Retries connection errors, timeouts, etc. up to 3 times.
    Does not retry business logic errors (custom exceptions, ValueError, etc.)
    """
    return RetryPolicy(
        initial_interval=timedelta(seconds=1),
        maximum_interval=timedelta(seconds=10),
        backoff_coefficient=2.0,
        maximum_attempts=3,
        non_retryable_error_types=NON_RETRYABLE_EXCEPTIONS,
    )
