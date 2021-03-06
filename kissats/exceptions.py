"""Kiss ATS Exceptions"""

import logging


logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())


class KissATSError(Exception):
    """Base exception for package."""
    pass


class MissingTestParamKey(KissATSError):
    """Required key missing in test parameter dictionary."""
    pass


class InvalidDut(KissATSError):
    """Invalid DUT selected for task."""
    pass


class InvalidATS(KissATSError):
    """Invalid ATS selected for task."""
    pass


class InvalidTask(KissATSError):
    """Invalid task requested."""
    pass


class FailedPrereq(KissATSError):
    """Task has a failed prereq task."""
    pass


class CriticalTaskFail(KissATSError):
    """A critical task has failed."""
    pass


class ResourceUnavailable(KissATSError):
    """Unable to reserve requested resource."""
    pass


class ServerCommandMissing(KissATSError):
    """Server command missing in server request."""
    pass


class ResourceRetryExceeded(KissATSError):
    """Too many task reservation retries."""
    pass


class ResourceRenewExceeded(KissATSError):
    """Too many task reservation renews."""
    pass


class InvalidConfigRequest(KissATSError):
    """An invalid request to reconfigure a resource was made."""
    pass


class ResourceNotReady(KissATSError):
    """Resource is not reserved or not ready."""
    pass


class InvalidResourceMode(KissATSError):
    """Invalid resource mode selected."""
    pass


class SchemaMisMatch(KissATSError):
    """Something didn't match the specified schema"""
    pass


class TaskPackageNotRegistered(KissATSError):
    """A method or function was attempted that requires
    a valid task package to be registered.
    """
    pass


class InvalidSchemaFile(KissATSError):
    """An invalid schema file was requested"""
    pass


class InvalidDataFile(KissATSError):
    """An invalid data file was requested"""
    pass


class UnsupportedRunMode(KissATSError):
    """An unsupported run mode was requested"""
    pass


class ObjectNotCallable(KissATSError):
    """A callable object was expected"""
    pass
