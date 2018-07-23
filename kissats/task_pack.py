"""Task Package manager"""


from collections import deque
import importlib
import json
import logging
import sys
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
                                TaskPackageNotRegistered)

from kissats.schemas import MASTER_SCHEMAS
from kissats.schemas import normalize_and_validate


logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())


class TaskPack(object):
    """Schedules and executes a group or package of kissats.BaseTask

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

        super(TaskPack, self).__init__()

        # switch case for resource modes
        # Tuple: use_scheduler, combine_resource, claim_per_task
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
        self._valid_task_result = None
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

        # build the queues
        self._test_que = deque()
        self._setup_que = deque()
        self._teardown_que = deque()
        self._delay_que = list()
        self._active_que = self._setup_que
        # do some init stuff
        self.params = init_params
        self.resource_mode = self.params.pop('resource_mode')
        self.thread_limit = self.params.pop('thread_limit')
        self.process_limit = self.params.pop('process_limit')
        self.ats_client = self.params.pop('ats_client')

        if self.params['test_groups']:
            for setup_task in self.setup_list:
                self.add_setup_task(setup_task)
            for teardown_task in self.teardown_list:
                self.add_teardown_task(teardown_task)
            self.test_groups = self.params.pop('test_groups')
            for test_group in self.test_groups:
                self.add_test_group(test_group)

    @property
    def task_pack(self):
        """The package containing the tasks to run

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
                pip_in_package(task_package)
                task_pack_stem = pathlib.Path(task_package).stem
                dist = task_pack_stem.split("-")[0]
                self._task_pack = importlib.import_module(dist)
            else:
                try:
                    self._task_pack = importlib.import_module(task_package)
                except ImportError:
                    if package_ver is not None:
                        _pip_str = task_package + package_ver
                    else:
                        _pip_str = task_package
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
            self.resource_mode = self._params.pop('resource_mode')

        return self._resource_mode

    @resource_mode.setter
    def resource_mode(self, new_mode):

        self._resource_mode = new_mode
        try:
            (self._use_scheduler,
             self._combine_resources,
             self._claim_per_task) = self._resource_modes_switch[new_mode]

        except KeyError:
            self._release_all_resources()
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
    def valid_task_result(self):
        """Valid result returns

        These are the only result values that will be considered a non-failure
        condition.

        """
        if self._valid_task_result is None:
            self._valid_task_result = self._params.pop('valid_task_result')

        return self._valid_task_result

    @valid_task_result.setter
    def valid_task_result(self, result_list):

        self._valid_task_result = result_list

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

        self._report_func = func

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
                        self._release_all_resources()
                        raise KissATSError("ATS Client package {0} "
                                           "failed to install".format(pip_str))
                    ats_module = importlib.import_module(ats_client_mod_name)

            else:
                # .. Todo: (BF) add check here to make sure new_ats_client is
                #          a valid ATS client class
                ats_module = importlib.import_module(self._ats_client)

            self._ats_client = ats_module.ATS_Client(**self.params['ats_client_args'])

        return self._ats_client

    @ats_client.setter
    def ats_client(self, new_ats_client):

        # .. Todo: (BF) add check here to make sure new_ats_client is
        #          a valid ATS client class

        self._ats_client = new_ats_client

    def report_result(self, name, description,
                      result, metadata):
        """Report results using a registered reporting function.

        If no reporting function is registered, result will be
        reported using the python built in logging module.

        Args:
            name(str): The name of the task
            description(str): A short description of the task
            result(str): the result of the task, IE: Passed, Failed, etc.
            metadata(str):  Any test metadata, typically a flattened format
                            such as JSON or YAML

        """

        if self._report_func is not None:
            self._report_func(name, description, result, metadata)
        else:
            logger.info("task %s self descibes as %s", name, description)
            logger.info("task %s result: %s", name, result)
            logger.info("task %s metadata: %s", name, metadata)

    def _record_result(self, task, result):
        """record result inside class, call report method

        Args:
            task(kissats.task.Task): The task that was executed
            result(dict): The results of the task

        """

        logger.info("reporting result of task %s", task.name)

        self._completed_tasks[task.name] = result['task_result']
        self.report_result(task.name,
                           task.params['description'],
                           result['task_result'],
                           result['task_metadata'])
        if result.get('multi_result') is not None:
            for sub_task_result in result['multi_result']:
                self.report_result(sub_task_result['name'],
                                   sub_task_result['description'],
                                   sub_task_result['task_result'],
                                   sub_task_result['task_metadata'])

    def clear_test_que(self):
        """clear the test queue"""

        self._test_que.clear()

    def clear_setup_que(self):
        """clear the setup queue"""

        self._setup_que.clear()

    def clear_teardown_que(self):
        """clear the teardown queue"""

        self._teardown_que.clear()

    def clear_delay_que(self):
        """clear the delay queue"""

        self._delay_que = list()

    def clear_all_que(self):
        """clear all task queues"""

        self.clear_setup_que()
        self.clear_test_que()
        self.clear_delay_que()
        self.clear_teardown_que()

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

        """

        self._active_que = self._test_que
        self._add_task_to_que(task, allow_dupe, top)

    def add_setup_task(self, task, allow_dupe=False, top=False):
        """add a task to the setup queue

        Args:
            task(kissats.task.Task or str or ModuleType): Task to add
            allow_dupe(bool): Allow the task to run multiple times
            top(bool): Place the task at the top of the queue

        see :func:`~add_test_task` for Task input handling
        and further allow_dupe explanation

        """

        self._active_que = self._setup_que
        self._add_task_to_que(task, allow_dupe, top)

    def add_teardown_task(self, task, allow_dupe=False, top=False):
        """add a task to the teardown queue

        Args:
            task(kissats.task.Task or str or ModuleType): Task to add
            allow_dupe(bool): Allow the task to run multiple times
            top(bool): Place the task at the top of the queue

        see :func:`~add_test_task` for Task input handling
        and further allow_dupe explanation

        """
        self._active_que = self._teardown_que
        self._add_task_to_que(task, allow_dupe, top)

    def _add_task_to_que(self, task, allow_dupe=False, top=False):
        """add a task to the active queue"""

        if task.__class__ is Task:
            task_to_add = task
        else:
            task_to_add = self._instantiate_task(task)

        if (task_to_add in self._active_que) and not allow_dupe:
            logger.warning("unable to add %s due to "
                           "task already in que", task_to_add.name)
            # it's already in the queue, return a True
            return True

        if self.dut is not None and not task_to_add.check_dut_valid():
            logger.warning("unable to add %s due to "
                           "invlaid DUT selected", task_to_add.name)
            return False

        if self.ats is not None and not task_to_add.check_ats_valid():
            logger.warning("unable to add %s due to "
                           "invlaid ATS selected", task_to_add.name)
            return False

        if top:
            self._active_que.appendleft(task_to_add)
            logger.info("%s added to top of que", task_to_add.name)
        else:
            self._active_que.append(task_to_add)
            logger.info("%s added to bottom of que", task_to_add.name)
        self._est_run_time += task_to_add.time_estimate

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
                    self._release_all_resources()
                    raise FailedPrereq(err_msg)
                return False

            if prereqs_needed:
                for prereq in prereqs_needed:
                    in_prereq = self._instantiate_task(prereq)
                    try:
                        self._active_que.remove(in_prereq)
                        self._est_run_time -= in_prereq.time_estimate
                        logger.info("%s removed from que", in_prereq.name)
                    except ValueError:
                        pass

                    if not self._add_task_to_que(prereq, top=True):
                        self._release_all_resources()
                        raise KissATSError("Critical error adding "
                                           "prereq task to que")
        return True

    def _instantiate_task(self, task):
        """Instantiate the Task class

        Args:
            task (str or module): If task is a str, attempt to import, if the
                                  first try fails, we will prepend with
                                  the package name and try again.
                                  If task is a loaded module, it will be
                                  passed directly to the Task class

        Raises:
            TaskPackageNotRegistered:
            InvalidTask:

        Returns:
            (Object)

        """

        if task.__class__ is str:

            try:
                imported_task = Task(task, self.params, self.ats_client)
            except ImportError:
                try:
                    if self.task_pack is None:
                        raise TaskPackageNotRegistered()
                    pack_task = "{0}.{1}".format(self.task_pack.__name__,
                                                 task)
                    imported_task = Task(pack_task, self.params, self.ats_client)
                except ImportError:
                    self._release_all_resources()
                    raise InvalidTask("{0} is an invalid task "
                                      "or failed to import".format(task))
        elif task.__class__ is ModuleType:
            imported_task = Task(task, self.params, self.ats_client)

        return imported_task

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

        self._test_groups.append(test_group)

        for test_task in self.get_seq_group(test_group, "test"):
            self.add_test_task(test_task)
        for setup_task in self.get_seq_group(test_group, "setup"):
            self.add_setup_task(setup_task)
        for teardown_task in self.get_seq_group(test_group, "teardown"):
            self.add_teardown_task(teardown_task)

    def get_seq_group(self, group_name, seq_name):
        """Get a list of tasks from a seq

        Args:
            group_name(str): Group to find
            seq_name (str): seq to check (setup, test or teardown)

        Returns:
            (list):

        """

        if self.task_pack is None:
            raise TaskPackageNotRegistered()
        try:
            seq = importlib.import_module("{0}.{1}".format(self.task_pack.__name__,
                                                           seq_name)) # flake8: noqa F841
        except ImportError:
            return list()
        if seq_name == "test":
            seq_name += "s"
        group = "get_{0}_{1}".format(group_name, seq_name)
        if callable(getattr(seq, group)):
            return getattr(seq, group)()

        return list()

    def check_prereqs(self, task):
        """Check if prereq tasks have been completed for a task

        Args:
            task (Task): the task to check

        Returns:
            (tuple):
                (list): prereqs needed
                (list): failed prereqs

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
                    if result in self.valid_task_result:
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

        WIll run all queue's in order:
            * setup
            * test
            * teardown

        Raises:
            KissATSError:
            ResourceUnavailable:

        """

        use_resources = False
        if self.ats_client and (self.resource_mode not in ['all', 'all_custom']):
            raise KissATSError("invalid Resoure Run Mode for run method")
        else:
            use_resources = True
            self._reserve_all_resources()
            last_available_time = time.time()
            for resource in self._all_resources:
                last_available_time = max(resource.start_time, last_available_time)

            time_now = time.time()
            while time.time() < last_available_time:
                time.sleep(last_available_time - time_now + 1)

            for resource in self.all_resources:
                try:
                    if not resource.claim_reservation():
                        raise ResourceUnavailable("Unable to claim resource "
                                                  "{0}".format(resource.resource_name))
                except (ResourceRetryExceeded, ResourceUnavailable):
                    self._release_all_resources()
                    raise ResourceUnavailable("Unable to claim resource "
                                              "{0}".format(resource.resource_name))
        self.run_setup_que()
        if self.schedule_func is not None:
            self.schedule_func(self._test_que)
        self.run_test_que()
        self.run_teardown_que()

        if use_resources:
            for resource in self.all_resources:
                resource.release_reservation()

    def run_setup_que(self):
        """run all tasks in setup queue"""

        self._active_que = self._setup_que
        if not self._claim_per_task:
            pass

        self._exec_active_que()

    def run_test_que(self):
        """run all tasks in test queue"""

        self._active_que = self._test_que
        self._exec_active_que()

    def run_teardown_que(self):
        """run all tasks in the teardown queue"""

        self._active_que = self._teardown_que
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
                    unavailable_resource = resource.resource_name
                    break
            if not all_reserved:
                self._release_all_resources()
                raise ResourceUnavailable("unable to reserve "
                                          "resource: {0}".format(unavailable_resource))

    def _release_all_resources(self):
        """Release all reserved resources"""

        for resource in self.all_resources:
            resource.release_reservation()

    def _delay_que_add_task(self, task):
        """place task(s) and reservation(s) in delay queue ordered on reservation time

        Args:
            task(kissats.task.Task): Task to add

        """

        last_available_time = 0.0
        first_expire_time = float(sys.maxsize)

        for reservation in task.resource_pre_res_list:
            last_available_time = max(reservation.start_time, last_available_time)
            first_expire_time = min(reservation.pre_res_expire, first_expire_time)

        que_entry = {'task': task,
                     'all_resource_ready': last_available_time,
                     'expire_time': first_expire_time}

        self._delay_que.append(que_entry)
        self._delay_que.sort(key=lambda k: k['all_resource_ready'], reverse=True)

    def _call_scheduler(self):
        """Call the registered scheduler/optimizer

        Passes the active queue to a previously registered function

        """

        if self.schedule_func is not None:
            self.schedule_func(self._active_que)

    def _exec_active_que(self):
        """Execute all tasks in the active queue"""

        if self._use_scheduler:
            self._call_scheduler()

        while True:
            result = dict()

            if self._delay_que:
                if self._delay_que[0]['expire_time'] >= time.time():
                    try:
                        task_to_delay = self._delay_que.pop(0)
                        logger.warning("Delaying task %s due to resource reservation(s) expire",
                                       task_to_delay.name)

                        if task_to_delay.add_resource_delay():
                            self._delay_que_add_task(task_to_delay)
                        else:
                            self._release_all_resources()
                            raise ResourceUnavailable

                    except (ResourceRetryExceeded, ResourceUnavailable):
                        warn_msg = ("Skipping test {0}, "
                                    "unable to reserve resources").format(
                                        task_to_delay.name)

                        logger.warning(warn_msg)

                        result['task_result'] = "Skipped"
                        result['task_metadata'] = {'message': warn_msg}
                        self._record_result(task_to_delay.name, result)

                    continue

                if self._delay_que[0]['all_resource_ready'] >= time.time():

                    task_to_run = self._delay_que.pop()

            else:
                try:
                    task_to_run = self._active_que.popleft()
                except IndexError:
                    break
            if not self.ignore_prereq:
                prereqs_needed, failed_prereqs = self.check_prereqs(task_to_run)
                if prereqs_needed:
                    # if we are here, something went wrong when the task was added to the queue
                    self._release_all_resources()
                    raise KissATSError("Fatal prereq error")
                if failed_prereqs:
                    warn_msg = ("Skipping test {0}, "
                                "pre-req(s) {1} failed").format(task_to_run.name,
                                                                failed_prereqs)
                    logger.warning(warn_msg)

                    result['task_result'] = "Skipped"
                    result['task_metadata'] = {'message': warn_msg}
                    self._record_result(task_to_run, result)
                    continue

            # reserve per task resources here, if task has a pre-reservation ID, pass that to
            # reservation system

            # if resource won't be available till later place in resource_delay_que

            # if resource isn't available, place at bottom of queue
            try:
                start_time = time.time()
                result = task_to_run.run_task()
                run_time = time.time() - start_time
            finally:
                # release per task resources here
                pass

            if result['task_metadata'].__class__ is dict:
                result['task_metadata']['run_time'] = run_time

            self._record_result(task_to_run, result)
            if result['task_result'] not in self.valid_task_result:
                if (task_to_run.params['stop_suite_on_fail'] or
                        ("setup" in task_to_run.name)):

                    err_msg = ("Critical task {0} failed, "
                               "testing terminated").format(task_to_run.name)
                    logger.error(err_msg)
                    self._release_all_resources()
                    raise CriticalTaskFail(err_msg)
            if self._delay_que:
                # if in resource available window, if window has passed, clear pre-reservation ID,
                # place back in active_que
                pass
