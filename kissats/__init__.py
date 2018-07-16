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
                                ResourceNotReady)

from kissats.ats_client import BaseATSClient
from kissats.task_pack import TaskPack



__version__ = None

with open("{0}\\VERSION".format(
    os.path.dirname(
        os.path.abspath(
            inspect.getfile(
                inspect.currentframe())))), 'r') as f:
    __version__ = f.readline()
