"""
Custom exception types for Data Nadhi workflows.

Non-retryable exceptions that should fail immediately and log to MinIO.
"""


class DataNadhiError(Exception):
    """Base exception for all Data Nadhi errors (non-retryable)"""
    pass


# Pipeline and Configuration Errors
class PipelineNotFoundError(DataNadhiError):
    """Pipeline not found in database"""
    pass


class WorkflowConfigNotFoundError(DataNadhiError):
    """Workflow configuration not found"""
    pass


class InvalidPipelineConfigError(DataNadhiError):
    """Pipeline configuration is invalid or malformed"""
    pass


# Transformation Errors
class TransformationError(DataNadhiError):
    """Error during data transformation"""
    pass


class FilterEvaluationError(DataNadhiError):
    """Error during filter evaluation"""
    pass


class InvalidTransformationError(DataNadhiError):
    """Transformation function not found or invalid"""
    pass


# Integration/Destination Errors
class IntegrationNotFoundError(DataNadhiError):
    """Integration connector or target not found"""
    pass


class UnsupportedIntegrationTypeError(DataNadhiError):
    """Integration type is not supported"""
    pass


class DestinationSendError(DataNadhiError):
    """Error sending data to destination (business logic error, not connection)"""
    pass


# Validation Errors
class InvalidInputDataError(DataNadhiError):
    """Input data is invalid or malformed"""
    pass


class MissingRequiredFieldError(DataNadhiError):
    """Required field is missing"""
    pass
