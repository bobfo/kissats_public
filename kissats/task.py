"""Kiss ATS Task"""

import abc
import logging
import importlib
import time

import six

from kissats.exceptions import (KissATSError,
                                MissingTestParamKey,
                                InvalidDut,
                                InvalidATS,
                                ResourceNotReady,
                                InvalidTask,
                                UnsupportedRunMode)

from kissats.ats_resource import ResourceReservation
from kissats.schemas import MASTER_SCHEMAS
from kissats.schemas import normalize_and_validate


logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())


@six.add_metaclass(abc.ABCMeta)
class BaseTask(object):
    """Base task class

    Args:
        task_in(Any): an object containing the appropriate functions
        global_params_in(dict): Global dictionary of parameters used
                                to configure the environment.
                                This dictionary will also be passed
                                to the registered task functions.

        schema_add (dict): additional schema definition to be applied
                           to task params

    """

    def __init__(self, task_in=None, global_params_in=None, schema_add=None):
        # type: (Optional[Any], Optional[dict], Optional[dict]) -> None  # noqa E501
        super(BaseTask, self).__init__()

    def __eq__(self, other):

        if other.__class__ is Task:
            return self.name == other.name
        elif other.__class__ is str:
            return self.name == other

        return self == other

    def __ne__(self, other):

        if other.__class__ is Task:
            return self.name != other.name
        elif other.__class__ is str:
            return self.name != other

        return self != other

    def __del__(self):
        self.release_resources()

    @property
    @abc.abstractmethod
    def name(self):
        # type: () -> str
        """The name of the task. This will be the name tracked and reported by the TaskPack"""

    @property
    @abc.abstractmethod
    def ats_client(self):
        # type: () -> kissats.BaseATSClient
        """ATS client class based on BaseATSClient"""

    @ats_client.setter
    @abc.abstractmethod
    def ats_client(self, new_client):
        # type: (Any) -> None

        pass

    @property
    @abc.abstractmethod
    def resource_list(self):
        # type: () -> list[Any]
        """List of resources needed for task"""

    @property
    @abc.abstractmethod
    def global_params(self):
        # type: () -> dict
        """Global Parameters to be passed to the task"""

    @global_params.setter
    @abc.abstractmethod
    def global_params(self, param_in):
        # type: (Any) -> None
        pass

    @property
    @abc.abstractmethod
    def missing_keys(self):
        # type: () -> list[str]
        """Missing parameter dictionary keys"""

    @property
    @abc.abstractmethod
    def run_mode(self):
        # type: () -> str
        """The mode to run the task in, normal, process or thread"""

    @run_mode.setter
    @abc.abstractmethod
    def run_mode(self, new_mode):
        # type: (str) -> None
        pass

    @property
    @abc.abstractmethod
    def time_estimate(self):
        # type: () -> float
        """estimated total run time"""

    @time_estimate.setter
    @abc.abstractmethod
    def time_estimate(self, new_est_time):
        # type: (Any) -> None
        pass

    @property
    @abc.abstractmethod
    def params(self):
        # type: () -> dict
        """Run parameters of the task.

        see :ref:`task_parameters_schema`.

        """

    @params.setter
    @abc.abstractmethod
    def params(self, params_in):
        # type: (Any) -> None
        pass

    @property
    @abc.abstractmethod
    def task_prereqs(self):
        # type: () -> list[str]
        """prereqs for the task"""

    @property
    @abc.abstractmethod
    def time_window(self):
        # type: () -> dict
        """Planned execution time"""

    @abc.abstractmethod
    def set_time_window(self, start_time, end_time):
        # type: (float, float) -> None
        """set the expected execution time of the task for reservation planning

        Args:
            start_time(float): Epoch time of expected start
                               Default: time.time() of function call
            end_time(float): Epoch time of expected completion.
                             Default: start_time + time_estimate
        """

    @abc.abstractmethod
    def reserve_resources(self):
        # type: () -> bool
        """Request reservations for all resources

        Returns:
            (bool):
                True if all resources were reserved

        """

    @abc.abstractmethod
    def claim_resources(self):
        # type: () -> bool
        """Claim all reservations"""

    @abc.abstractmethod
    def release_resources(self):
        # type: () -> None
        """Release all reservations and clear time window"""

    @abc.abstractmethod
    def resource_delay(self):
        # type: () -> bool
        """delay all resource reservations

        Returns:
            (bool):
                True if all resources are reserved

        """

    @abc.abstractmethod
    def check_requires(self):
        # type: () -> bool
        """Verify all requirements for executing the task are met

        Returns:
            (bool): True if all requirements are met

        """

    @abc.abstractmethod
    def check_resources_ready(self):
        # type: () -> bool
        """check if all resources are reserved for the task

        Returns:
            (bool):
                True if all resources are ready

        """

    @abc.abstractmethod
    def check_dut_valid(self):
        # type: () -> bool
        """check if task is valid for the DUT"""

    @abc.abstractmethod
    def check_ats_valid(self):
        # type: () -> bool
        """check if task is valid for the ATS"""

    @abc.abstractmethod
    def task_setup(self):
        # type: () -> None
        """Setup action for this task."""

    @abc.abstractmethod
    def task_main(self):
        # type: () -> dict
        """The main task"""

    @abc.abstractmethod
    def task_teardown(self):
        # type: () -> None
        """Teardown action for this task"""

    @abc.abstractmethod
    def run_task(self):
        # type: (string) -> dict
        """run the task

        If the task module has a task_setup, task_setup will be
        executed first.

        If the task module has a task_teardown, task_teardown will
        be executed after the task_main function. If the task params key
        always_teardown is set to True, task_teardown will run
        regardless of the exit status of task_setup and the run function.

        Warning:
            If the property always_teardown is True, task_teardown will execute
            even if task_setup or run raise an exception.

        """


class Task(BaseTask):
    """a task and it's parameters

    Args:
        task_in(Any): The importable name of the task to run or
                      a module containing the appropriate functions

                      Note:
                        If a str, should be in package.module format

        global_params_in(dict): Global dictionary of parameters used
                                to configure the environment.
                                This dictionary will also be passed
                                to the registered task functions.

        schema_add (dict): (Optional) Additional
                           `Cerberus <http://docs.python-cerberus.org/en/stable/>`_
                           schema definition to be applied to the init_params

    """

    def __init__(self, task_in=None, global_params_in=None, schema_add=None):
        # type: (Optional[Any], Optional[dict], Optional[kissats.BaseATSClient], Optional[dict]) -> None  # noqa E501
        super(Task, self).__init__(task_in, global_params_in, schema_add)

        self._task_mod = None
        self._global_params = dict()
        self._ats_client = None
        self._name = None
        self._addition_schema = schema_add
        self._run_mode = None

        # these are built by the class
        self._missing_keys = None
        self._time_window = {'start': None, 'finish': None}
        self._resource_list = None

        # these are retrieved from task
        self._task_prereqs = None
        self._time_estimate = None
        self._priority = None
        self._thread_safe = False
        self._process_safe = False
        self._params = None

        if global_params_in is not None:
            self.global_params = global_params_in

        if task_in is not None:
            self.task_mod = task_in

    @property
    def name(self):
        # type: () -> str
        """The name of the task.

        This will be the name tracked
        and reported by the TaskPack

        """
        if self._name is None:
            self._name = self.params['name']

        return self._name

    @name.setter
    def name(self, new_name):
        # type: (str) -> None

        self._name = new_name

    @property
    def ats_client(self):
        """Instantiated ATS client class based on BaseATSClient"""

        return self._ats_client

    @ats_client.setter
    def ats_client(self, new_client):

        self._ats_client = new_client

    @property
    def resource_list(self):
        """List of resources needed for task"""

        if self._resource_list is None:
            self._init_resource_list()

        return self._resource_list

    @property
    def global_params(self):
        """Global Parameters to be passed to the task"""

        if self._global_params is None:
            raise KissATSError("Global params must be set first!")

        return self._global_params

    @global_params.setter
    def global_params(self, param_in):

        self._global_params = param_in

    @property
    def missing_keys(self):
        """Missing parameter dictionary keys"""

        self._missing_keys = list()
        task_keys = self.params['req_param_keys']
        for key in task_keys:
            if key not in self.global_params:
                self._missing_keys.append(key)
        return self._missing_keys

    @property
    def time_estimate(self):
        # type: () -> float
        """estimated total run time"""

        if self._time_estimate is None:
            self._time_estimate = self.params['est_task_time']

        return self._time_estimate

    @time_estimate.setter
    def time_estimate(self, new_est_time):
        # type: (float) -> None

        self._time_estimate = new_est_time

    @property
    def params(self):
        # type: (...) -> dict
        """Run parameters of the task.

        This will call get_params on the task module registered,
        expecting a dict conforming with the :ref:`task_parameters_schema`.

        """

        if self._params is None:
            raise KissATSError("Task params must be set first!")

        return self._params

    @params.setter
    def params(self, params_in):
        _valid_schema = MASTER_SCHEMAS.task_param_schema
        if self._addition_schema is not None:
            _valid_schema.update(self._addition_schema)

        self._params = normalize_and_validate(params_in, _valid_schema)

    @property
    def task_prereqs(self):
        """prereqs for the task"""

        if self._task_prereqs is None:
            self._task_prereqs = self.params['prereq_tasks']
        return self._task_prereqs

    @property
    def time_window(self):
        # type: () -> dict
        """Planned execution time"""

        if self._time_window.get('start') is None:
            self.set_time_window()

        return self._time_window

    @property
    def task_mod(self):
        """Any class, module or duck

        Must contain the minimum task execution
        attributes run and get_params.

        Alternately accepts a string and will import the module.

        Note:

            If a string is used, it should be in importable
            package.module format.

        """

        return self._task_mod

    @task_mod.setter
    def task_mod(self, task_in):

        if task_in.__class__ is str:
            self._task_mod = importlib.import_module(task_in)
        else:
            self._task_mod = task_in

        if (not hasattr(self._task_mod, "task_main") or
                not hasattr(self._task_mod, "get_params")):

            raise InvalidTask("{0} is an invaid task".format(task_in))

        self.params = self.get_params()

    @property
    def run_mode(self):
        # type: () -> str
        """The mode to run the task in, normal, process or thread"""

        if self._run_mode is None:
            self._run_mode = "normal"
        return self._run_mode

    @run_mode.setter
    def run_mode(self, new_mode):
        # type: (str) -> None

        self._run_mode = new_mode

    def _init_resource_list(self):
        """Build the list of resources"""

        self._resource_list = list()
        if self.ats_client is not None:
            for resource in self.params['exclusive_resources']:
                resource_to_add = ResourceReservation(resource,
                                                      self.ats_client,
                                                      "exclusive",
                                                      self.params['max_resource_retry'],
                                                      self.params['max_resource_wait'])
                resource_to_add.resource_config = self.params['resource_configs'].get(resource)
                self._resource_list.append(resource_to_add)

            for resource in self._params['shared_resources']:
                resource_to_add = ResourceReservation(resource,
                                                      self.ats_client,
                                                      "exclusive",
                                                      self.params['max_resource_retry'],
                                                      self.params['max_resource_wait'])
                resource_to_add.resource_config = self.params['resource_configs'].get(resource)
                self._resource_list.append(resource_to_add)

    def set_time_window(self, start_time=None, end_time=None):
        # type: (Optional[float], Optional[float]) -> None
        """set the expected execution time of the task for reservation planning

        Args:
            start_time(float): Epoch time of expected start
                               Default: time.time() of function call
            end_time(float): Epoch time of expected completion.
                             Default: start_time + time_estimate
        """

        if start_time is None:
            start_time = time.time()

        if end_time is None:
            end_time = start_time + self.time_estimate

        self._time_window = {'start': start_time, 'finish': end_time}

    def reserve_resources(self):
        # type: () -> bool
        """Request reservations for all resources

        Returns:
            (bool):
                True if all resources were reserved

        """

        return_bool = True
        if self.ats_client is not None:
            for resource in self._resource_list:
                request_status = resource.request_reservation(self._time_window['start'],
                                                              self._time_window['finish'])
                return_bool = return_bool and request_status

        return return_bool

    def claim_resources(self):
        # type: () -> bool
        """Claim all reservations"""

        if self.ats_client is not None:
            for resource in self._resource_list:
                if not resource.claim_reservation():
                    # resource claim failed
                    pass

    def release_resources(self):
        # type: () -> None
        """Release all reservations and clear time window"""

        if self.ats_client is not None:
            self._time_window = {'start': None, 'finish': None}
            for resource in self._resource_list:
                resource.release_reservation()

    def resource_delay(self):
        # type: () -> bool
        """Delay all resource reservations

        Warning:
            this method will reset the time_window

        Returns:
            (bool):
                True if all resources are reserved

        """

        for resource in self._resource_list:
            resource.add_retry_count()

        self.release_resources()
        return self.reserve_resources()

    def check_requires(self):
        # type: () -> bool
        """Verify all requirements for executing the task are met

        Returns:
            (bool): True if all requirements are met

        Raises:
            MissingTestParamKey:
            InvalidDut:
            InvalidATS:
            ResourceNotReady:

        """

        logger.debug("checking requirements for task")
        if self.missing_keys:
            raise MissingTestParamKey(self.missing_keys)
        if not self.check_dut_valid():
            raise InvalidDut(self.global_params.get('dut'))
        if not self.check_ats_valid():
            raise InvalidATS(self.global_params.get('ats'))
        if not self.check_resources_ready():
            raise ResourceNotReady

        return True

    def check_resources_ready(self):
        # type: () -> bool
        """check if all resources are reserved for the task

        Returns:
            (bool):
                True if an ATS Client is registered and
                all resources are reserved, will also
                return True if an ATS Client is not registered

        """

        if self.ats_client is not None:
            for resource in self.resource_list:
                if resource.reservation_id is None:
                    return False

        return True

    def check_dut_valid(self):
        """check if task is valid for the DUT

        If DUT is not specified in global params
        this method will return True

        """

        if self.global_params.get('dut') is None:
            return True

        if "any" in (dut.lower() for dut in self.params['valid_duts']):
            return True

        if self.global_params.get('dut') in self.params['valid_duts']:
            return True
        logger.warning("valid DUT check: %s not in %s",
                       self.global_params.get('dut'),
                       self.params['valid_duts'])
        return False

    def check_ats_valid(self):
        """check if task is valid for the ATS

        If ATS is not specified in global params
        this method will return True

        """

        if self.global_params.get('ats') is None:
            return True

        if "any" in (ats.lower() for ats in self.params['valid_ats']):
            return True

        if self.global_params.get('ats') in self.params['valid_ats']:
            return True
        logger.warning("valid ATS check: %s not in %s",
                       self.global_params.get('ats'),
                       self.params['valid_ats'])

        return False

    def _run_task_func(self, func):
        """run a function contained in the task

        Args:
            func (builtin_function_or_method): the function to run

        Returns:
            (dict)

        Raises:
            UnsupportedRunMode:

        """

        if self.run_mode == "normal":
            results = self._run_normal(func)
        elif self.run_mode == "process":
            results = self._run_process(func)
        elif self.run_mode == "thread":
            results = self._run_thread(func)
        else:
            raise UnsupportedRunMode

        return results

    def _run_normal(self, func):
        """run task in normal mode

        Args:
            func (builtin_function_or_method): the function to run

        Returns:
            (dict)

        """
        results = dict()
        try:
            results = func(self.global_params)
            if results is None:
                results = dict()
        except (SystemExit, KeyboardInterrupt):
            raise
        except Exception, err:
            logger.exception(err)
            results['result'] = "Exception"
            results['metadata'] = {"exception_details": err}
        return results

    def _run_thread(self, func):
        """run task in a thread

        Args:
            func (builtin_function_or_method): the function to run

        """
        raise NotImplementedError

    def _run_process(self, func):
        """run task in a new process

        Args:
            func (builtin_function_or_method): the function to run

        """
        raise NotImplementedError

    def get_params(self):
        # type: () -> dict
        """The parameters for executing the task"""

        return self._task_mod.get_params(self.global_params)

    def task_setup(self):
        # type: () -> None
        """Setup action for this task."""

        if hasattr(self._task_mod, "task_setup"):
            logger.info("executing task setup")
            setup_result = self._run_task_func(self._task_mod.task_setup)

            if setup_result.get('test_status') == "Exception":
                raise KissATSError("Setup Exception "
                                   "{0} see log for full stack "
                                   "trace".format(setup_result['test_result']))

    def task_main(self):
        # type: () -> dict
        """The main task function"""

        results = self._run_task_func(self._task_mod.task_main)

        return results

    def task_teardown(self):
        # type: () -> None
        """Teardown action for this task"""

        if hasattr(self._task_mod, "task_teardown"):
            logger.info("executing task teardown")
            teardown_result = self._run_task_func(self._task_mod.task_teardown)
            if teardown_result.get('test_status') == "Exception":
                raise KissATSError("Teardown Exception "
                                   "{0} see log for full stack "
                                   "trace".format(teardown_result['test_result']))

    def run_task(self):
        """run the task

        If the task module has a task_setup, task_setup will be
        executed first.

        If the task module has a task_teardown, task_teardown will
        be executed after the run function. If the task params key
        always_teardown is set to True, task_teardown will run
        regardless of the exit status of task_setup and the run function.

        Warning:
            If the class property always_teardown is True, task_teardown
            will execute even if task_setup or task_main raise an exception.

        """

        results = dict()
        logger.info("checking requirements for task %s", self.name)
        self.check_requires()

        logger.info("executing task %s", self.name)
        run_teardown = True

        try:
            logger.info("executing task %s task_setup", self.name)
            self.task_setup()
            logger.info("executing task %s task_main", self.name)
            results = self.task_main()
            logger.info("task results: %s", results)

        except (SystemExit, KeyboardInterrupt):
            run_teardown = False

        except Exception:
            if not self.params['always_teardown']:
                run_teardown = False
            raise

        finally:
            if run_teardown:
                logger.info("executing task %s task_teardown", self.name)
                self.task_teardown()

        return results
