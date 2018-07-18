"""
KISS ATS

"""
# flake8: noqa F401

import os
import inspect


from kissats.exceptions import (KissATSError,
                                MissingTestParamKey,
                                InvalidDut,
                                InvalidATS,
                                InvalidTask,
                                FailedPrereq,
                                CriticalTaskFail,
                                ResourceUnavailable,
                                ServerCommandMissing,
                                ResourceRetryExceeded,
                                InvalidConfigRequest,
                                ResourceRenewExceeded,
                                ResourceNotReady,
                                InvalidResourceMode)

from kissats.ats_client import BaseATSClient
from kissats.task_pack import TaskPack


__version__ = "1.0.0a2"
