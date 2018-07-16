"""
Kiss ATS Task

"""


import logging
import importlib
import time

from kissats import (KissATSError,
                     MissingTestParamKey,
                     InvalidDut,
                     InvalidATS,
                     ResourceNotReady)

from kissats.ats_resource import ResourceReservation


logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())


class Task(object):
    """
    a task to run

    Args:
        task_name(str): The importable name of the task to run

                          .. note:: package.module format

        param_input(dict): Gloabal Dictonary of parameters used to configure the environment.
                           This dictionary will also be passed to all functions of the task.
        ats_client_in(kissats.BaseATSClient): instantiated ATS client class based on BaseATSClient

    """

    def __init__(self, task_name, param_input, ats_client_in=None):
        # type: (str, dict, kissats.BaseATSClient) -> None
        super(Task, self).__init__()
        self._task_mod = importlib.import_module(task_name)
        self.task_name = task_name
        if param_input is not None:
            self.param = param_input
        self._ats_client = ats_client_in
        self._gloabal_params = dict()
        self._task_prereqs = None
        self._missing_keys = None
        self._task_params = None
        self._time_estimate = None
        self._priority = None
        self._time_window = {'start': None, 'finish': None}
        self._thread_safe = False
        self._process_safe = False
        self._resource_list = None

    def __eq__(self, other):

        if other.__class__ is Task:
            return self.task_name == other.task_name
        elif other.__class__ is str:
            return self.task_name == other

        return self == other

    def __ne__(self, other):

        if other.__class__ is Task:
            return self.task_name != other.task_name
        elif other.__class__ is str:
            return self.task_name != other

        return self != other

    def __del__(self):
        self.release_resources()

    @property
    def ats_client(self):
        """
        instantiated ATS client class based on BaseATSClient

        """

        return self._ats_client

    @ats_client.setter
    def ats_client(self, new_client):

        self._ats_client = new_client

    @property
    def resource_list(self):
        """
        List of resources need for task

        """

        if self._resource_list is None:
            self._init_resource_list()

        return self._resource_list

    @property
    def gloabal_params(self):
        """
        Parameters to be passed to the task

        """

        return self._gloabal_params

    @gloabal_params.setter
    def param(self, param_in):

        self._gloabal_params = param_in

    @property
    def missing_keys(self):
        """
        Missing parameter dictionary keys

        """

        self._missing_keys = list()
        task_keys = self.task_params['req_param_keys']
        for key in task_keys:
            if key not in self.param:
                self._missing_keys.append(key)
        return self._missing_keys

    @property
    def time_estimate(self):
        # type: () -> float
        """
        estimated total run time

        """
        if self._time_estimate is None:
            self._time_estimate = self.task_params['est_task_time']

        return self._time_estimate

    @time_estimate.setter
    def time_estimate(self, new_est_time):
        # type: (float) -> None

        self._time_estimate = new_est_time

    @property
    def task_params(self):
        # type: (...) -> dict
        """
        params of the task, this will call get_params
        on the task, expecting a dict with:

        Required Keys:
            * name
            * description

        Optional Keys and defaults:
            * stop_suite_on_fail, True
            * exclusive_resources, list()
            * shared_resources, list()
            * resource_configs, dict()
            * max_resource_wait, 3600
            * max_resource_retry, 5
            * thread_safe, False
            * process_safe, False
            * valid_ats, ['any']
            * valid_duts, ['any']
            * req_param_keys, list()
            * optional_param_keys, list()
            * prereq_tasks, list()
            * est_test_time, 3600
            * extra_metadata, None

        Note:
            resource_configs if present must have a key with the same name
            as each resource listed in exclusive_resources and shared_resources.
            The value of this key will be passed to the ATS resource manager.

        """

        if self._task_params is None:
            self._task_params = self._task_mod.get_params(self.gloabal_params)
            if (self._task_params['name'] is None or
                    self._task_params['description'] is None):

                raise MissingTestParamKey("all tasks must have a name and description")

            self._task_params.setdefault('stop_suite_on_fail', True)
            self._task_params.setdefault('exclusive_resources', list())
            self._task_params.setdefault('shared_resources', list())
            self._task_params.setdefault('resource_configs', dict())
            self._task_params.setdefault('max_resource_wait', 3600)
            self._task_params.setdefault('max_resource_retry', 5)
            self._task_params.setdefault('thread_safe', False)
            self._task_params.setdefault('process_safe', False)
            self._task_params.setdefault('valid_ats', ['any'])
            self._task_params.setdefault('valid_duts', ['any'])
            self._task_params.setdefault('req_param_keys', list())
            self._task_params.setdefault('optional_param_keys', list())
            self._task_params.setdefault('prereq_tasks', list())
            self._task_params.setdefault('est_task_time', 3600)
            self._task_params.setdefault('always_teardown', False)
            self._task_params.setdefault('extra_metadata', None)

            logger.debug("task params: %s", self._task_params)
        return self._task_params

    @property
    def task_prereqs(self):
        """
        prereqs for the task

        """

        if self._task_prereqs is None:
            self._task_prereqs = self.task_params.get('prereq_tasks')
        return self._task_prereqs

    def _init_resource_list(self):
        """
        Build the list of resources

        """
        if self.ats_client is not None:
            self._resource_list = list()
            for resource in self.task_params['exclusive_resources']:
                resource_to_add = ResourceReservation(resource,
                                                      self.ats_client,
                                                      "exclusive",
                                                      self.task_params['max_resource_retry'],
                                                      self.task_params['max_resource_wait'])
                resource_to_add.resource_config = self.task_params['resource_configs'].get(resource)
                self._resource_list.append(resource_to_add)

            for resource in self._task_params['shared_resources']:
                resource_to_add = ResourceReservation(resource,
                                                      self.ats_client,
                                                      "exclusive",
                                                      self.task_params['max_resource_retry'],
                                                      self.task_params['max_resource_wait'])
                resource_to_add.resource_config = self.task_params['resource_configs'].get(resource)
                self._resource_list.append(resource_to_add)

    @property
    def time_window(self):
        # type: () -> dict
        """
        Planned execution time

        """

        if self._time_window.get('start') is None:
            self.set_time_window()

        return self._time_window

    def set_time_window(self, start_time=None, end_time=None):
        # type: (Optional[float], Optional[float]) -> None
        """
        set the expected execution time of the task for reservation planning

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
        # type: () -> Bool
        """
        Request reservations for all resources

        """
        if self.ats_client is not None:
            for resource in self._resource_list:
                resource.request_reservation(self._time_window['start'],
                                             self._time_window['finish'])

    def claim_resources(self):
        # type: () -> Bool
        """
        Claim all reservations

        """
        if self.ats_client is not None:
            pass

    def release_resources(self):
        # type: () -> None
        """
        Release all reservations

        """

        if self.ats_client is not None:
            for resource in self._resource_list:
                resource.release_reservation()

    def _run_task_func(self, func, run_mode):
        """
        run a function contained in the task

        Args:
            func (builtin_function_or_method): the function to run
            run_mode(str): The mode to run the task in, normal, process or thread
                           Note:
                                Currently normal mode is the only mode supported.

        """

        if run_mode == "normal":
            results = self._run_normal(func)
        elif run_mode == "process":
            results = self._run_process(func)
        elif run_mode == "thread":
            results = self._run_thread(func)
        else:
            raise KissATSError("selected run mode is not supported")

        return results

    def _run_normal(self, func):
        """
        run task in normal mode

        Args:
            func (builtin_function_or_method): the function to run

        """
        results = dict()
        try:
            results = func(self.gloabal_params)
            if results is None:
                results = dict()
        except (SystemExit, KeyboardInterrupt):
            raise
        except Exception, err:
            logger.exception(err)
            results['test_status'] = "Exception"
            results['test_result'] = err
        return results

    def _run_thread(self, func):
        """
        run task in a thread

        Args:
            func (builtin_function_or_method): the function to run

        """
        raise NotImplementedError

    def _run_process(self, func):
        """
        run task in a new process

        Args:
            func (builtin_function_or_method): the function to run

        """
        raise NotImplementedError

    def run_task(self, run_mode="normal"):
        """
        run the task

        If the task module has a task_setup, task_setup will be
        executed first.

        If the task module has a task_teardown, task_teardown will
        be executed after the run function. If the task params key
        always_teardown is set to True, task_teardown will run
        regardless of the exit status of task_setup and the run function.

        Warning:
            If always_teardown is True, task_teardown will execute
            even if task_setup or run throw an exception.

        Args:
            run_mode(str): The mode to run the task in, normal, process or thread
                           Note:
                                Currently normal mode is the only mode supported.

        """

        task_return = dict()
        logger.info("checking requirements for task %s", self.task_name)
        self.check_requires()

        logger.info("executing task %s", self.task_name)
        run_teardown = True

        try:
            if hasattr(self._task_mod, "task_setup"):
                logger.info("executing task setup")
                func = self._task_mod.task_setup
                setup_result = self._run_task_func(func, run_mode)
                if setup_result.get('test_status') == "Exception":
                    raise KissATSError("Setup Exception "
                                       "{0} see log for full stack "
                                       "trace".format(setup_result['test_result']))

            func = self._task_mod.run
            results = self._run_task_func(func, run_mode)
            logger.info("task results: %s", results)

        except (SystemExit, KeyboardInterrupt):
            run_teardown = False

        except Exception:
            if not self.task_params['always_teardown']:
                run_teardown = False
            raise

        finally:
            if run_teardown and hasattr(self._task_mod, "task_teardown"):
                logger.info("executing task teardown")
                func = self._task_mod.task_teardown
                teardown_result = self._run_task_func(func, run_mode)
                if teardown_result.get('test_status') == "Exception":
                    raise KissATSError("Teardown Exception "
                                       "{0} see log for full stack "
                                       "trace".format(teardown_result['test_result']))

        logger.info("task return: %s", task_return)

        return task_return

    def check_requires(self):
        # type: () -> bool
        """
        Verify all requirements for executing the task are met

        Returns:
            (bool): True if all requirements are met

        """

        logger.debug("checking requirements for task")
        if self.missing_keys:
            raise MissingTestParamKey(self.missing_keys)
        if not self.check_dut_valid():
            raise InvalidDut(self.gloabal_params.get('dut_type'))
        if not self.check_ats_valid():
            raise InvalidATS(self.gloabal_params.get('ats'))
        if not self.check_resources_ready():
            raise ResourceNotReady

        return True

    def check_resources_ready(self):
        # type: () -> bool
        """
        check if all resources are reserved for the task

        Returns:
            (bool): True if an ATS Client is regestered and
                    all resources are reserved, will also
                    return True if an ATS Client is not regestered

        """

        if self.ats_client is not None:
            for resource in self.resource_list:
                if resource.reservation_id is None:
                    return False

        return True

    def check_dut_valid(self):
        """
        check if task is valid for the DUT

        """

        if "any" in (dut.lower() for dut in self.task_params['valid_duts']):
            return True

        if self.gloabal_params.get('dut_type') in self.task_params['valid_duts']:
            return True
        logger.warning("valid DUT check: %s not in %s",
                       self.gloabal_params.get('dut_type'),
                       self.task_params['valid_duts'])
        return False

    def check_ats_valid(self):
        """
        check if task is valid for the ATS

        """

        if "any" in (ats.lower() for ats in self.task_params['valid_ats']):
            return True

        if self.gloabal_params.get('ats') in self.task_params['valid_ats']:
            return True
        logger.warning("valid ATS check: %s not in %s",
                       self.gloabal_params.get('ats'),
                       self.task_params['valid_ats'])

        return False
