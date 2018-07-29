"""Task Package manager"""


import importlib
import json
import logging
import time
from types import ModuleType

import pathlib2 as pathlib


from kissats.common import pip_in_package
from kissats.task import Task
from kissats.exceptions import (KissATSError,
                                InvalidTask,
                                FailedPrereq,
                                CriticalTaskFail,
                                ResourceRetryExceeded,
                                ResourceUnavailable,
                                InvalidResourceMode,
                                TaskPackageNotRegistered,
                                ObjectNotCallable,
                                SchemaMisMatch)

from kissats.schemas import MASTER_SCHEMAS
from kissats.schemas import normalize_and_validate
from kissats.queues import PackQues


logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())


class PackParams(object):
    """Holds the parameters of the task package

    see :ref:`global_parameters_schema`

    Args:
        init_params (dict): Initialization parameter dict. see XXXXX
        schema_add (dict): (Optional) Additional
                           `Cerberus <http://docs.python-cerberus.org/en/stable/>`_
                           schema definition to be applied to the init_params

    Note:
        If task_package is not supplied in the init_params, tasks must be added
        using the appropriate add task method. Any method that depends on a valid
        task_package will raise.

    """

    def __init__(self, init_params=None, schema_add=None):

        super(PackParams, self).__init__()

        # switch case for resource modes
        # Tuple: use_scheduler, combine_resources, claim_per_task
        self._resource_modes_switch = {
            "all": (False, True, False),
            "per_task_separate": (False, False, True),
            "per_task_combine": (False, True, True),
            "custom_all": (True, True, False),
            "custom_separate": (True, False, True),
            "custom_combine": (True, True, True)
            }

        if init_params is None:
            init_params = dict()

        self._addition_schema = schema_add
        self._params = None

        # these are set from the init_params
        self._task_pack = None
        self._ats_client = None
        self._thread_limit = None
        self._process_limit = None
        self._resource_mode = None
        self._ignore_prereq = None
        self._valid_result = None
        self._dut = None
        self._ats = None
        self._run_mode = None
        self._test_groups = None
        # a flat representation of the params
        self._json_params = None
        # these optional values are set by the user
        self._report_func = None
        self._schedule_func = None
        self._run_mode = None

        # these are set by the property resource_mode
        self._use_scheduler = False
        self._combine_resources = True
        self._claim_per_task = False

        # these are built by the class
        self._completed_tasks = dict()
        self._est_run_time = 3600
        self._all_resources = list()
        self._setup_list = None
        self._teardown_list = None

        # do some init stuff
        self.params = init_params
        self.resource_mode = self.params.pop('resource_mode')
        self.thread_limit = self.params.pop('thread_limit')
        self.process_limit = self.params.pop('process_limit')
        self.ats_client = self.params.pop('ats_client')

    @property
    def task_pack(self):
        """The Python package containing the tasks to run

        Also accepts a wheel, distribution name must match
        the import name!

        Note:
            Don't be like PyYAML:
            distribution name is PyYAML
            import name is yaml

        Warning:
            This property can only be set once!

        """

        if (self._task_pack is None and
                self.params.get('task_package') is not None):

            task_package = self.params.pop('task_package')
            package_ver = self.params.pop('package_version')

            if [c in task_package for c in ["\\", "/", ":"] if c in task_package]:
                # looks like a file, install it
                logger.debug("pip installing %s", task_package)
                pip_in_package(task_package)
                task_pack_stem = pathlib.Path(task_package).stem
                dist = task_pack_stem.split("-")[0]
                logger.debug("importing task package %s", dist)
                self._task_pack = importlib.import_module(dist)
            else:
                try:
                    logger.debug("importing task package %s", task_package)
                    self._task_pack = importlib.import_module(task_package)
                except ImportError:
                    if package_ver is not None:
                        _pip_str = task_package + package_ver
                    else:
                        _pip_str = task_package
                    logger.debug("pip installing %s", _pip_str)
                    if pip_in_package(_pip_str):
                        raise KissATSError("package {0} "
                                           "failed to install".format(_pip_str))
                    self._task_pack = importlib.import_module(task_package)

        return self._task_pack

    @task_pack.setter
    def task_pack(self, new_pack, version=None):

        if self._task_pack is not None:
            raise KissATSError("task_pack can only be set once!")

        self.params['task_package'] = new_pack
        self.params['package_version'] = version

    @property
    def dut(self):
        """The Device Under Test"""

        if (self._dut is None and
                self.params.get('dut') is not None):

            self._dut = self.params.pop('dut')

        return self._dut

    @dut.setter
    def dut(self, dut_in):
        self._dut = dut_in

    @property
    def run_mode(self):
        # type: () -> str
        """The global run mode, normal, process or thread"""

        if self._run_mode is None:
            self._run_mode = "normal"
        return self._run_mode

    @run_mode.setter
    def run_mode(self, new_mode):
        # type: (str) -> None

        self._run_mode = new_mode

    @property
    def test_groups(self):
        # type: () -> list
        """The scheduled test groups"""

        if self._test_groups is None:
            self._test_groups = list()

        return self._test_groups

    @test_groups.setter
    def test_groups(self, new_group_list):
        # type: (list) -> None

        self._test_groups = new_group_list

    @property
    def ats(self):
        """The Automated Test System used to perform the testing"""

        if (self._ats is None and
                self.params.get('ats') is not None):

            self._ats = self.params.pop('ats')

        return self._ats

    @ats.setter
    def ats(self, ats_in):
        self._ats = ats_in

    @property
    def est_run_time(self):
        """Estimated total run time in seconds"""

        return self._est_run_time

    @est_run_time.setter
    def est_run_time(self, new_run_time):

        self._est_run_time = new_run_time

    @property
    def resource_mode(self):
        """Resource reservation mode"""

        if self._resource_mode is None:
            self.resource_mode = self.params.pop('resource_mode')

        return self._resource_mode

    @resource_mode.setter
    def resource_mode(self, new_mode):

        self._resource_mode = new_mode
        try:
            (self._use_scheduler,
             self._combine_resources,
             self._claim_per_task) = self._resource_modes_switch[new_mode]

        except KeyError:
            raise InvalidResourceMode(new_mode)

    @property
    def all_resources(self):
        """A list of all resources needed for tasks currently in any queue

        Resource should be kissats.ResourceReservation

        """

        return self._all_resources

    @property
    def params(self):
        """The global parameter dict"""

        return self._params

    @params.setter
    def params(self, params_in):

        _valid_schema = dict()
        if self._addition_schema is not None:
            _valid_schema = self._addition_schema
        _valid_schema.update(MASTER_SCHEMAS.global_param_schema)

        self._params = normalize_and_validate(params_in, _valid_schema)
        self._params['task_pack'] = self

    @property
    def json_params(self):
        """Keys in the parameter dictionary formated in JSON

        Note:
            Any key that cannot be flattened by json.dumps will be excluded

        """
        temp_params = dict()
        for k, v in self.params.iteritems():
            try:
                json.dumps(v)
            except TypeError:
                continue
            temp_params[k] = v

        self._json_params = json.dumps(temp_params)

        return self._json_params

    @property
    def valid_result(self):
        """Valid result returns

        These are the only result values that will be considered a non-failure
        condition.

        """
        if self._valid_result is None:
            self._valid_result = self.params.pop('valid_result')

        return self._valid_result

    @valid_result.setter
    def valid_result(self, result_list):

        self._valid_result = result_list

    @property
    def setup_list(self):
        """The list of setup tasks required by the task package.

        Will call the get_global_setup function from the seq_setup
        module in the task_package to populate the list.

        """

        if self.task_pack is None:
            raise TaskPackageNotRegistered()

        if self._setup_list is None:
            try:
                setup_seq = importlib.import_module("{0}.{1}".format(self.task_pack.__name__,
                                                                     "seq_setup"))
                self._setup_list = setup_seq.get_global_setup(self.params)
            except ImportError:
                logger.info("Task package does not have a seq_setup")
                self._setup_list = list()
        return self._setup_list

    @property
    def teardown_list(self):
        """The list of teardown tasks required by the task package

        Will call the get_global_teardown function from the seq_teardown
        module in the task_package to populate the list.

        """

        if self.task_pack is None:
            raise TaskPackageNotRegistered()

        if self._teardown_list is None:
            try:
                setup_seq = importlib.import_module("{0}.{1}".format(self.task_pack.__name__,
                                                                     "seq_teardown"))
                self._teardown_list = setup_seq.get_global_teardown(self.params)
            except ImportError:
                logger.info("Test package does not have a global teardown")
                self._teardown_list = list()
        return self._teardown_list

    @property
    def ignore_prereq(self):
        """when set, will ignore prereqs"""

        if self._ignore_prereq is None:
            self.ignore_prereq = self.params.pop("ignore_prereq")
        return self._ignore_prereq

    @ignore_prereq.setter
    def ignore_prereq(self, state):

        if state:
            logger.warning("Prereq checking is disabled!!!!!")
            self._ignore_prereq = True
        else:
            logger.info("Prereq checking enabled")
            self._ignore_prereq = False

    @property
    def completed_tasks(self):
        """dict of completed tasks and their results"""

        return self._completed_tasks

    @property
    def report_func(self):
        """Function to report results"""

        return self._report_func

    @report_func.setter
    def report_func(self, func):

        if callable(func):
            self._report_func = func
        else:
            raise ObjectNotCallable("{0} is not callable".format(func))

    @property
    def schedule_func(self):
        """Function to schedule/order tasks"""

        return self._schedule_func

    @schedule_func.setter
    def schedule_func(self, func):

        self._schedule_func = func

    @property
    def thread_limit(self):
        # type: () -> int
        """Max additional threads to use"""

        if self._thread_limit is None:
            self._thread_limit = self.params.pop('thread_limit')

        return self._thread_limit

    @thread_limit.setter
    def thread_limit(self, new_limit):

        self._thread_limit = new_limit

    @property
    def process_limit(self):
        # type: () -> int
        """Max additional processes to use"""

        if self._process_limit is None:
            self._process_limit = self.params.pop('process_limit')

        return self._process_limit

    @process_limit.setter
    def process_limit(self, new_limit):

        self._process_limit = new_limit

    @property
    def ats_client(self):
        """The ATS client for communication with the ATS"""
        # TODO (BF): Need to better document this

        if self._ats_client is not None and self._ats_client.__class__ is str:
            if self._ats_client.lower() == "auto":
                ats_client_mod_name = "ats_{}.client".format(self.params['ats'])
                try:
                    ats_module = importlib.import_module(ats_client_mod_name)
                except ImportError:

                    pip_str = "ats_{}".format(self.params['ats'])
                    if pip_in_package(pip_str):
                        raise KissATSError("ATS Client package {0} "
                                           "failed to install".format(pip_str))
                    ats_module = importlib.import_module(ats_client_mod_name)

            else:
                # .. Todo: (BF) add check here to make sure new_ats_client is
                #          a valid ATS client class
                ats_module = importlib.import_module(self._ats_client)

            self._ats_client = ats_module.ATS_Client(**self.params['ats_client_kwargs'])

        return self._ats_client

    @ats_client.setter
    def ats_client(self, new_ats_client):

        # .. Todo: (BF) add check here to make sure new_ats_client is
        #          a valid ATS client class

        self._ats_client = new_ats_client


class TaskPack(PackParams):
    """Schedules and executes a group or package of kissats.BaseTask

    see :ref:`global_parameters_schema`

    Args:
        init_params (dict): Initialization parameter dict. see XXXXX
        schema_add (dict): (Optional) Additional
                           `Cerberus <http://docs.python-cerberus.org/en/stable/>`_
                           schema definition to be applied to the init_params
        que_class (object): Class or object containing the queues, see
                            :class:`~BaseQues`

    Note:
        If task_package is not supplied in the init_params, tasks must be added
        using the appropriate add task method. Any method that depends on a valid
        task_package will raise.

    """

    def __init__(self, init_params=None, schema_add=None, que_class=PackQues):

        # build the queues
        self._ques = que_class()

        # do some init stuff
        super(TaskPack, self).__init__(init_params, schema_add)

        if self.params['test_groups']:
            for setup_task in self.setup_list:
                self.add_setup_task(setup_task)
            for teardown_task in self.teardown_list:
                self.add_teardown_task(teardown_task)
            for test_group in self.params.pop('test_groups'):
                self.add_test_group(test_group)

    def __del__(self):
        logger.info("Class destruction, releasing all resources")
        self._release_all_resources()

    def report_result(self, result):
        """Report results using a registered reporting function.

        If no reporting function is registered, result will be
        reported using the python built in logging module.

        Args:
            result(dict): see reporting_schema for details

        """

        if self.report_func is not None:
            self.report_func(result)
        else:
            logger.info("task %s result: %s", result['name'], result['result'])
            logger.info("task %s self descibes as %s", result['name'], result['description'])
            logger.info("task %s metadata: %s", result['name'], result['metadata'])

    def _record_result(self, task, result):
        """record result inside class, call report method

        Args:
            task(kissats.task.Task): The task that was executed
            result(dict): The results of the task

        """

        logger.info("reporting result of task %s", task.name)
        self._completed_tasks[task.name] = result['result']

        if result.get('multi_result') is not None:
            for sub_result in result.pop('multi_result'):
                sub_result = normalize_and_validate(sub_result, MASTER_SCHEMAS.reporting_schema)
                self.report_result(sub_result)

        result['name'] = task.name
        result['description'] = task.params['description']
        result = normalize_and_validate(result, MASTER_SCHEMAS.reporting_schema)
        self.report_result(result)

    def clear_test_que(self):
        """clear the test queue"""

        self._ques.set_active_que("test")
        self._ques.clear_active_que()

    def clear_setup_que(self):
        """clear the setup queue"""

        self._ques.set_active_que("setup")
        self._ques.clear_active_que()

    def clear_teardown_que(self):
        """clear the teardown queue"""

        self._ques.set_active_que("teardown")
        self._ques.clear_active_que()

    def clear_delay_que(self):
        """clear the delay queue"""

        self._ques.set_active_que("delay")
        self._ques.clear_active_que()

    def clear_all_que(self):
        """clear all task queues"""

        self._ques.clear_all_que()

    def add_test_task(self, task, allow_dupe=False, top=False):
        """add a task to the test queue

        Args:
            task(kissats.task.Task or str or ModuleType): Task to add
            allow_dupe(bool): Allow the task to run multiple times
                              If set to false and the task is already
                              in the queue a warning will be logged and
                              processing will continue.

            top(bool): Place the task at the top of the queue

        Warning:
            If dut and/or ats are not set in the TaskPack, the
            dut and/or ats will not be verified and the task
            will be added to the queue

        Note:
            Task input handling:

            * If task is a kissats.task.Task based class it
              will be added directly to the queue

            * If task is a str we will an attempt will be made
              to import by the Task class, if the import fails,
              we will prepend with the package name and try again.

            * If task is a ModuleType, it will be
              passed directly to the Task class

            * If the dut from the global params is not listed in the
              task params key valid_duts the task will not be added
              and processing will continue

            * If the ats from the global params is not listed in the
              task params key valid_ats the task will not be added
              and processing will continue

        Returns:
            bool: True if in the queue

        """

        self._ques.set_active_que("test")
        return self._add_task_to_que(task, allow_dupe, top)

    def add_setup_task(self, task, allow_dupe=False, top=False):
        """add a task to the setup queue

        Args:
            task(kissats.task.Task or str or ModuleType): Task to add
            allow_dupe(bool): Allow the task to run multiple times
            top(bool): Place the task at the top of the queue

        see :func:`~add_test_task` for Task input handling
        and further allow_dupe explanation

        Returns:
            bool: True if in the queue

        """

        self._ques.set_active_que("setup")
        return self._add_task_to_que(task, allow_dupe, top)

    def add_teardown_task(self, task, allow_dupe=False, top=False):
        """add a task to the teardown queue

        Args:
            task(kissats.task.Task or str or ModuleType): Task to add
            allow_dupe(bool): Allow the task to run multiple times
            top(bool): Place the task at the top of the queue

        see :func:`~add_test_task` for Task input handling
        and further allow_dupe explanation

        Returns:
            bool: True if in the queue

        Raises:
            FailedPrereq:
            KissATSError:

        """
        self._ques.set_active_que("teardown")
        return self._add_task_to_que(task, allow_dupe, top)

    def _add_task_to_que(self, task, allow_dupe=False, top=False):
        """add a task to the active queue"""

        task_to_add = self._load_and_validate_task(task)

        if self.dut is not None and not task_to_add.check_dut_valid():
            logger.warning("unable to add %s due to "
                           "invlaid DUT selected", task_to_add.name)
            return False

        if self.ats is not None and not task_to_add.check_ats_valid():
            logger.warning("unable to add %s due to "
                           "invlaid ATS selected", task_to_add.name)
            return False

        if allow_dupe or not self._ques.in_active_que(task_to_add):
            self._ques.add_to_active_que(task_to_add, top)
            self._est_run_time += task_to_add.time_estimate
        else:
            # already in queue and dupe not allowed
            return True

        if self._combine_resources:
            for idx, resource in enumerate(task_to_add.resource_list):
                try:
                    self_idx = self._all_resources.index(resource)
                    task_to_add.resource_list[idx] = self._all_resources[self_idx]
                except ValueError:
                    self._all_resources.append(resource)

        if self.ignore_prereq:
            logger.info("Prereq check skipped")

        else:
            prereqs_needed, failed_prereqs = self.check_prereqs(task_to_add)
            if failed_prereqs:
                err_msg = ("unable to add {0} due to "
                           "failed prereq task(s) {1}").format(task_to_add.name,
                                                               failed_prereqs)
                logger.warning(err_msg)
                if task_to_add.params['stop_suite_on_fail']:
                    raise FailedPrereq(err_msg)
                return False

            if prereqs_needed:
                for prereq in prereqs_needed:
                    in_prereq = self._load_and_validate_task(prereq)
                    try:
                        self._ques.remove_from_active_que(in_prereq)
                        self._est_run_time -= in_prereq.time_estimate
                    except ValueError:
                        pass

                    if not self._add_task_to_que(prereq, top=True):
                        raise KissATSError("Critical error adding "
                                           "prereq task to que")
        return True

    def _load_and_validate_task(self, task):
        """optionally load and then validate the task

        Args:
            task (str, module or object):
                * If task is a str, attempt to import, if the
                  first try fails, we will prepend with
                  the package name and try again.

                * If task is a loaded module, it will be
                  passed directly to the Task class.

                * If task is an object, it will be checked for
                  the callable attributes get_params and
                  task_main
        Raises:
            TaskPackageNotRegistered:
            InvalidTask:

        Returns:
            Object: The validated task

        """

        if task.__class__ is str:

            try:
                new_task = Task(task, self.params, self.ats_client)
            except ImportError:
                try:
                    if self.task_pack is None:
                        raise TaskPackageNotRegistered()
                    pack_task = "{0}.{1}".format(self.task_pack.__name__,
                                                 task)
                    logger.debug("attempting to import %s", pack_task)
                    new_task = Task(pack_task, self.params, self.ats_client)
                except ImportError:
                    raise InvalidTask("{0} is an invalid task "
                                      "or failed to import".format(task))
        elif task.__class__ is ModuleType:
            new_task = Task(task, self.params, self.ats_client)

        else:
            new_task = task

        # check that task has the min required methods/functions
        try:
            if not callable(new_task.get_params):
                raise InvalidTask("get_params is not callable, unable to add task")
        except AttributeError:
            raise InvalidTask("Task does not have a get_params attribute")

        try:
            if not callable(new_task.task_main):
                raise InvalidTask("task_main is not callable, unable to add task")
        except AttributeError:
            raise InvalidTask("Task does not have a task_main attribute")

        return new_task

    def add_test_group(self, test_group):
        """Add all tests in the test group to the test queue

        If a corresponding group specific setup and teardown exists,
        they will also be added to the appropriate queue.

        Args:
            test_group (str): Test group to add to the test queue.

                              Note:
                                There must be a corresponding
                                get_<test_group>_tests function
                                in the test package's seq_test

        Raises:
            TaskPackageNotRegistered:

        """

        if self.task_pack is None:
            raise TaskPackageNotRegistered()

        logger.info("adding test group %s", test_group)
        for test_task in self.get_seq_group(test_group, "test"):
            self.add_test_task(test_task)
        for setup_task in self.get_seq_group(test_group, "setup"):
            self.add_setup_task(setup_task)
        for teardown_task in self.get_seq_group(test_group, "teardown"):
            self.add_teardown_task(teardown_task)

        self.test_groups.append(test_group)

    def get_seq_group(self, group_name, seq_name):
        """Get a list of tasks from a seq

        Args:
            group_name(str): Group to find
            seq_name (str): seq to check (setup, test or teardown)

        Returns:
            list: List of tasks

        """

        if self.task_pack is None:
            raise TaskPackageNotRegistered
        try:
            import_name = "{0}.seq_{1}".format(self.task_pack.__name__,
                                               seq_name)
            logger.debug("importing %s", import_name)

            seq = importlib.import_module(import_name)

        except ImportError:
            return list()
        if seq_name == "test":
            seq_name += "s"
        group = "get_{0}_{1}".format(group_name, seq_name)
        try:
            if callable(getattr(seq, group)):
                return getattr(seq, group)(self.params)
            else:
                logger.warning("%s in %s is not callable", group, seq)
        except AttributeError:
            logger.debug("%s does not have a function or method named %s", seq, group)

        return list()

    def check_prereqs(self, task):
        """Check if prereq tasks have been completed for a task

        Args:
            task (Task): the task to check

        Returns:
            tuple:

                * (list): prereqs needed
                * (list): failed prereqs

        """

        prereqs_needed = list()
        failed_prereqs = list()
        prereqs = task.task_prereqs
        logger.debug("%s prereqs: %s", task.name, prereqs)
        # Todo (BF) Clean this up
        for prereq in prereqs:
            prereq_run = False
            prereq_pass = False
            for c_test, result in self._completed_tasks.iteritems():
                logger.debug("prereq compare: %s prereq "
                             " to complete task %s", prereq, c_test)
                if ((prereq == c_test) or
                        (("{0}.{1}".format(self.task_pack.__name__,
                                           prereq)) == c_test)):
                    prereq_run = True
                    if result in self.valid_result:
                        prereq_pass = True
            if prereq_run:
                if not prereq_pass:
                    failed_prereqs.append(prereq)
            else:
                prereqs_needed.append(prereq)

        return prereqs_needed, failed_prereqs

    def run_all_que(self):
        """Run all queue's

        If ATS client is registered and resource mode is set to
        "all" or "custom_all", all resources will be reserved and
        claimed. When complete, all resources will be released.

        If resource mode is set to a custom mode, the registered
        schedular function will be called before execution.

        Will run all queue's in order:
            * setup
            * test
            * teardown

        Raises:
            KissATSError:
            ResourceUnavailable:

        """

        use_resources = False
        if self.ats_client and (self.resource_mode in ['all', 'all_custom']):
            use_resources = True
            self._reserve_all_resources()
            last_available_time = 0.0
            for resource in self._all_resources:
                last_available_time = max(resource.start_time, last_available_time)

            time_now = time.time()
            while time.time() < last_available_time:
                time.sleep(last_available_time - time_now + 1)

            for resource in self.all_resources:
                try:
                    if not resource.claim_reservation():
                        raise ResourceUnavailable("Unable to claim resource "
                                                  "{0}".format(resource.name))
                except (ResourceRetryExceeded, ResourceUnavailable):
                    raise ResourceUnavailable("Unable to claim resource "
                                              "{0}".format(resource.name))
        if self.ats_client and (self.resource_mode in ['per_task_combine', 'custom_combine']):
            raise NotImplementedError
        if self.ats_client and (self.resource_mode in ['per_task_separate', 'custom_separate']):
            raise NotImplementedError

        if self._use_scheduler:
            self.call_scheduler()

        self.run_setup_que()
        self.run_test_que()
        self.run_teardown_que()

        if use_resources:
            for resource in self.all_resources:
                resource.release_reservation()

    def run_setup_que(self):
        """run all tasks in setup queue"""

        self._ques.set_active_que("setup")
        if not self._claim_per_task:
            pass

        self._exec_active_que()

    def run_test_que(self):
        """run all tasks in test queue"""

        self._ques.set_active_que("test")
        self._exec_active_que()

    def run_teardown_que(self):
        """run all tasks in the teardown queue"""

        self._ques.set_active_que("teardown")
        self._exec_active_que()
        if not self._claim_per_task:
            pass

    def _reserve_all_resources(self):
        """Reserve all resources"""

        start_time = time.time() + 60
        end_time = start_time + self.est_run_time + (.20 * self.est_run_time)

        all_reserved = True
        for resource in self.all_resources:
            if not resource.request_reservation(start_time, end_time, False):
                all_reserved = False
                break
        if not all_reserved:
            last_slot_start = 0.0
            for resource in self.all_resources:
                resource.release_reservation()
                last_slot_start = max(resource.get_next_avail_time()['avail_start'])

            start_time = last_slot_start
            end_time = start_time + self.est_run_time + (.20 * self.est_run_time)
            all_reserved = True
            unavailable_resource = ""
            for resource in self.all_resources:
                if not resource.request_reservation(start_time, end_time, False):
                    all_reserved = False
                    unavailable_resource = resource.name
                    break
            if not all_reserved:
                raise ResourceUnavailable("unable to reserve "
                                          "resource: {0}".format(unavailable_resource))

    def _release_all_resources(self):
        """Release all reserved resources"""

        for resource in self.all_resources:
            resource.release_reservation()

    def call_scheduler(self):
        """Call the registered scheduler/optimizer

        Passes all queues to a previously registered function

        """

        if self.schedule_func is not None:
            self.schedule_func(self._ques)

    def _exec_active_que(self):
        """Execute all tasks in the active queue"""

        while True:
            result = dict()

            if self._ques.delay_que_len:
                if self._ques.peek_delay()['expire_time'] >= time.time():
                    try:
                        task_to_delay = self._ques.delay_que.pop()
                        logger.warning("Delaying task %s due to resource reservation(s) expire",
                                       task_to_delay.name)

                        if task_to_delay.add_resource_delay():
                            self._ques.delay_que_add_task(task_to_delay)
                        else:
                            raise ResourceUnavailable
                    except (ResourceRetryExceeded, ResourceUnavailable):
                        warn_msg = ("Skipping test {0}, "
                                    "unable to reserve resources").format(
                                        task_to_delay.name)

                        logger.warning(warn_msg)

                        result['result'] = "Skipped"
                        result['metadata'] = {'message': warn_msg}
                        self._record_result(task_to_delay.name, result)
                    continue

                if self._ques.peek_delay()['all_resource_ready'] >= time.time():
                    task_to_run = self._ques.pop_delay()

            else:
                try:
                    task_to_run = self._ques.popleft_active()
                except IndexError:
                    if self._ques.delay_que_len:
                        time.sleep(self._ques.peek_delay()['all_resource_ready'] - time.time())
                    else:
                        break
            if not self.ignore_prereq:
                prereqs_needed, failed_prereqs = self.check_prereqs(task_to_run)
                if prereqs_needed:
                    # if we are here, something went wrong when the task was added to the queue
                    raise KissATSError("Fatal prereq error")
                if failed_prereqs:
                    warn_msg = ("Skipping test {0}, "
                                "pre-req(s) {1} failed").format(task_to_run.name,
                                                                failed_prereqs)
                    logger.warning(warn_msg)

                    result['result'] = "Skipped"
                    result['metadata'] = {'message': warn_msg}
                    self._record_result(task_to_run, result)
                    continue

            # reserve per task resources here, if task has a pre-reservation ID, pass that to
            # reservation system

            # if resource won't be available till later place in resource_delay_que

            # if resource isn't available, place at bottom of queue

            start_time = time.time()
            result = task_to_run.run_task()
            run_time = time.time() - start_time
            try:
                result = normalize_and_validate(result, MASTER_SCHEMAS.task_return_schema)
            except SchemaMisMatch as err:
                new_msg = "task {0} invalid return: {1}".format(task_to_run.name, err.message)
                raise SchemaMisMatch(new_msg)

            # release per task resources here

            result['metadata']['run_time'] = run_time
            result['metadata']['est_task_time'] = task_to_run.time_estimate
            result['metadata']['run_time_delta'] = task_to_run.time_estimate - run_time

            self._record_result(task_to_run, result)
            if result['result'] not in self.valid_result:
                if (task_to_run.params['stop_suite_on_fail'] or
                        ("setup" in task_to_run.name)):

                    err_msg = ("Critical task {0} failed, "
                               "testing terminated").format(task_to_run.name)
                    logger.error(err_msg)
                    raise CriticalTaskFail(err_msg)
            if self._ques.delay_que_len:
                # if in resource available window, if window has passed, clear pre-reservation ID,
                # place back in active_que
                pass
