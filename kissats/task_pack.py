"""
Task package

"""

from collections import deque
import importlib
import json
import logging
import sys
import time
from types import ModuleType


from kissats.common import pip_in_package
from kissats.task import Task
from kissats import (KissATSError,
                     InvalidTask,
                     FailedPrereq,
                     CriticalTaskFail,
                     ResourceRetryExceeded,
                     ResourceUnavailable,
                     InvalidResourceMode)


logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())


class TaskPack(object):
    """
    Imports the task package and executes tasks

    Args:
        task_package (str): importable test/task package name
        task_version (str): (Optonal) PIP compatible version string IE: >=1.0.0
        params_input (dict): (Optonal) paramater dict for all tasks
        test_group_in (str): (Optonal) test group to execute
        ats_client (str): (Optonal) ats_client module containing the ATS_Client
                          class to import and instantiate for resource
                          management, if None, no client will be used.

                          Note:
                             if set to "auto", we will attempt to first
                             import an ATS client based on the ATS name
                             provided in the params_input, if that fails,
                             we will attempt to pip install, then
                             import, if that fails a KissATSError will
                             be raised

        resource_mode (str): (Optonal) Resource reservation mode:

                                Warning:
                                    "all" is the only mode currently implemented

                                * "all" Will reserve all resources needed
                                  by all tasks for the estimated duration
                                  of the run. **This is the default mode**

                                * "per_task_separate" Will reserve resources
                                  on a task by task basis as each task is
                                  run. Each task will hold its own set of
                                  resources.

                                * "per_task_combine" Will reserve resources
                                  on a task by task basis as each task is
                                  run. Resources common to tasks
                                  will be consolidated.

                                * "custom_all" The que selected for execution
                                  will be passed to the function registered
                                  with schedule_func for ordering. Resource
                                  management will be handled the same as
                                  "all"

                                * "custom_separate" The que selected for execution
                                  will be passed to the function registered
                                  with schedule_func for ordering. Resource
                                  management will be handled the same as
                                  "per_task_separate"

                                * "custom_combine" The que selected for execution
                                  will be passed to the function registered
                                  with schedule_func for ordering. Resource
                                  management will be handled the same as
                                  "per_task_combine"

                                Note:
                                    For all custom modes, a scheduler function
                                    must be regesiterd with schedule_func before
                                    calling any run que methods.

    """

    def __init__(self, task_package, task_version=None, params_input=None,
                 test_group_in=None, ats_client=None, resource_mode="all"):

        super(TaskPack, self).__init__()
        try:
            self._task_pack = importlib.import_module(task_package)
        except ImportError:
            if task_version is not None:
                pip_str = task_package + task_version
            else:
                pip_str = task_package
            if pip_in_package(pip_str):
                 raise KissATSError("package {0} "
                                   "failed to install".format(pip_str))
            self._task_pack = importlib.import_module(task_package)

        self._setup_list = None
        self._task_list = None
        self._teardown_list = None
        self._params = None
        self._task_prereqs = None
        self._setup_prereqs = None
        self._ignore_prereq = None
        self._report_func = None
        self._schedule_func = None
        self._completed_tasks = dict()
        self._valid_task_result = None
        self._ats_client = ats_client
        self._json_params = None
        self._thread_limit = 5
        self._process_limit = 5
        self._est_run_time = 3600
        self._all_resources = list()
        self._use_scheduler = False
        self._combine_resources = True
        self._claim_per_task = False
        self._resource_mode = None
        self.resource_mode = resource_mode
        self._test_que = deque()
        self._setup_que = deque()
        self._teardown_que = deque()
        self._delay_que = list()

        self.params = params_input
        self._active_que = self._setup_que
        if test_group_in is not None:
            self.add_test_group(test_group_in)


        # Tuple: use_scheduler, combine_resource, claim_per_task
        self._resource_modes_switch = {
            "all": (False, True, False),
            "per_task_separate": (False, False, True),
            "per_task_combine": (False, True, True),
            "custom_all": (True, True, False),
            "custom_separate": (True, False, True),
            "custom_combine": (True, True, True)
            }

    @property
    def est_run_time(self):
        """
        Estimated total run time in seconds

        """

        return self._est_run_time

    @est_run_time.setter
    def est_run_time(self, new_run_time):

        self._est_run_time = new_run_time

    @property
    def resource_mode(self):
        """
        Resource reservation mode

        """

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
        """
        A list of all resources (ResourceReservation)
        needed for tasks in all que's

        """

        return self._all_resources

    @property
    def params(self):
        """
        The global parameter dict

        """
        if self._params is not None:
            self._params['task_pack'] = self

        return self._params

    @params.setter
    def params(self, params_in):

        self._params = params_in

    @property
    def json_params(self):
        """
        All keys in the parameter dictionary with
        values that can be flattened to JSON

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
        """
        Valid result returns from tasks indicating a non-failure
        condition.

        """
        if self._valid_task_result is None:
            self._valid_task_result = ["Passed", "Completed", "Skipped"]

        return self._valid_task_result

    @valid_task_result.setter
    def valid_task_result(self, result_list):

        self._valid_task_result = result_list

    @property
    def setup_list(self):
        """
        The list of setup tasks required by the task package.

        Will call the get_global_setup function from the seq_setup
        module in the task_package to populate the list.

        """
        if self._setup_list is None:
            setup_seq = importlib.import_module("{0}.{1}".format(self._task_pack.__name__,
                                                                 "seq_setup"))
            self._setup_list = setup_seq.get_global_setup(self.params)
        return self._setup_list

    @property
    def teardown_list(self):
        """
        The list of teardown tasks required by the task package

        Will call the get_global_teardown function from the seq_teardown
        module in the task_package to populate the list.

        """
        if self._setup_list is None:
            try:
                setup_seq = importlib.import_module("{0}.{1}".format(self._task_pack.__name__,
                                                                     "seq_teardown"))
                self._teardown_list = setup_seq.get_global_teardown(self.params)
            except ImportError:
                logger.info("Test package does not have a global teardown")
                self._teardown_list = list()
        return self._teardown_list

    @property
    def ignore_prereq(self):
        """
        when set, will ignore prereqs

        """

        if self._ignore_prereq is None:
            self._ignore_prereq = False
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
        """
        dict of completed tasks and their results

        """

        return self._completed_tasks

    @property
    def report_func(self):
        """
        Function to report results

        """

        return self._report_func

    @report_func.setter
    def report_func(self, func):

        self._report_func = func

    @property
    def schedule_func(self):
        """
        Function to schedule/order tasks

        """

        return self._schedule_func

    @schedule_func.setter
    def schedule_func(self, func):

        self._schedule_func = func

    @property
    def thread_limit(self):
        # type: () -> int
        """
        Function to report results

        """

        return self._thread_limit

    @thread_limit.setter
    def thread_limit(self, new_limit):

        self._thread_limit = new_limit

    @property
    def process_limit(self):
        # type: () -> int
        """
        Function to report results

        """

        return self._process_limit

    @process_limit.setter
    def process_limit(self, new_limit):

        self._process_limit = new_limit

    @property
    def ats_client(self):
        """
        The ATS client for communication with the ATS



        """

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
                #          a vaild ATS client class
                ats_module = importlib.import_module(self._ats_client)

            self._ats_client = ats_module.ATS_Client(**self.params['ats_client_args'])

        return self._ats_client

    @ats_client.setter
    def ats_client(self, new_ats_client):

        # .. Todo: (BF) add check here to make sure new_ats_client is
        #          a vaild ATS client class

        self._ats_client = new_ats_client

    def report_result(self, name, description,
                      result, metadata):
        """
        Report results using a registered reporting function.
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
        """
        record result inside class, call report method

        Args:
            task(kissats.task.Task): The task that was executed
            result(dict): The results of the task

        """

        logger.info("reporting result of task %s", task.task_name)

        self._completed_tasks[task.task_name] = result['task_result']
        self.report_result(task.task_name,
                           task.task_params['description'],
                           result['task_result'],
                           result['task_metadata'])
        if result.get('multi_result') is not None:
            for sub_task_result in result['multi_result']:
                self.report_result(sub_task_result['name'],
                                   sub_task_result['description'],
                                   sub_task_result['task_result'],
                                   sub_task_result['task_metadata'])

    def clear_test_que(self):
        """
        clear the test que

        """

        self._test_que.clear()

    def clear_setup_que(self):
        """
        clear the setup que

        """

        self._setup_que.clear()

    def clear_teardown_que(self):
        """
        clear the teardown que

        """

        self._teardown_que.clear()

    def clear_delay_que(self):
        """
        clear the delay que

        """

        self._delay_que = list()

    def clear_all_que(self):
        """
        clear all task que's

        """

        self.clear_setup_que()
        self.clear_test_que()
        self.clear_delay_que()
        self.clear_teardown_que()

    def add_test_task(self, task, allow_dupe=False, top=False):
        """
        add a task to the test que

        Args:
            task(kissats.task.Task or str or ModuleType): Task to add
            allow_dupe(bool): Allow the task to run multiple times
                              If set to false and the task is already
                              in the que a warning will be logged and
                              processing will continue.

            top(bool): Place the task at the top of the que

        Note:
            Task input handling:

            * If task is a kissats.task.Task based class it
              will be added directly to the que

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
        """
        add a task to the setup que

        Args:
            task(kissats.task.Task or str or ModuleType): Task to add
            allow_dupe(bool): Allow the task to run multiple times
            top(bool): Place the task at the top of the que

        see :func:`~add_test_task` for Task input handling
        and further allow_dupe explanation

        """

        self._active_que = self._setup_que
        self._add_task_to_que(task, allow_dupe, top)

    def add_teardown_task(self, task, allow_dupe=False, top=False):
        """
        add a task to the teardown que

        Args:
            task(kissats.task.Task or str or ModuleType): Task to add
            allow_dupe(bool): Allow the task to run multiple times
            top(bool): Place the task at the top of the que

        see :func:`~add_test_task` for Task input handling
        and further allow_dupe explanation

        """
        self._active_que = self._teardown_que
        self._add_task_to_que(task, allow_dupe, top)

    def _add_task_to_que(self, task, allow_dupe=False, top=False):
        """
        add a task to the active que

        """

        if task.__class__ is Task:
            task_to_add = task
        else:
            task_to_add = self._instantiate_task(task)

        if (task_to_add in self._active_que) and not allow_dupe:
            logger.warning("unable to add %s due to "
                           "task already in que", task_to_add.task_name)
            # it's already in the que, return a True
            return True

        if not task_to_add.check_dut_valid():
            logger.warning("unable to add %s due to "
                           "invlaid DUT selected", task_to_add.task_name)
            return False

        if not task_to_add.check_ats_valid():
            logger.warning("unable to add %s due to "
                           "invlaid ATS selected", task_to_add.task_name)
            return False

        if top:
            self._active_que.appendleft(task_to_add)
            logger.info("%s added to top of que", task_to_add.task_name)
        else:
            self._active_que.append(task_to_add)
            logger.info("%s added to bottom of que", task_to_add.task_name)
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
                           "failed prereq task(s) {1}").format(task_to_add.task_name,
                                                               failed_prereqs)
                logger.warning(err_msg)
                if task_to_add.task_params['stop_suite_on_fail']:
                    self._release_all_resources()
                    raise FailedPrereq(err_msg)
                return False

            if prereqs_needed:
                for prereq in prereqs_needed:
                    in_prereq = self._instantiate_task(prereq)
                    try:
                        self._active_que.remove(in_prereq)
                        self._est_run_time -= in_prereq.time_estimate
                        logger.info("%s removed from que", in_prereq.task_name)
                    except ValueError:
                        pass

                    if not self._add_task_to_que(prereq, top=True):
                        self._release_all_resources()
                        raise KissATSError("Critical error adding "
                                           "prereq task to que")

        return True

    def _instantiate_task(self, task):
        """
        Instantiate the Task class

        Args:
            task (str or module): If task is a str, attempt to import, if the
                                  first try failes, we will prepend with
                                  the package name and try again.
                                  If task is a loaded module, it will be
                                  passed directly to the Task class

        """

        if task.__class__ is str:
            try:
                imported_task = Task(task, self.params, self.ats_client)
            except ImportError:
                try:
                    pack_task = "{0}.{1}".format(self._task_pack.__name__,
                                                 task)
                    imported_task = Task(pack_task, self.params, self.ats_client)
                except ImportError:
                    self._release_all_resources()
                    raise InvalidTask("{0} is an invalid task "
                                      "or failed to import".format(task))
        elif task.__class__ is ModuleType:
            imported_task = Task(task, self.params, self.ats_client)

        return imported_task

    def add_setup(self):
        """
        Add setup tasks to the task que

        """

        for setup_task in self.setup_list:
            self.add_setup_task(setup_task)

    def add_test_group(self, test_group):
        """
        add all tests in the test group to the test que

        Args:
            test_group (str): Test group to add to the test que.

                              Note:
                                There must be a corresponding
                                get_<test_group>_tests function
                                in the test package's seq_test

        """
        tg_to_add = self.get_test_group(test_group)
        for test_task in tg_to_add:
            self.add_test_task(test_task)

    def get_test_group(self, test_group):
        """
        get a list of tests from a test_group

        Args:
            test_group (str): Test group to query

                              Note:
                                There must be a coresponding get_<test_group>_tests
                                in the test package's seq_test

        Returns:
                (list): list of test groups from seq_test

        """

        test_seq = importlib.import_module("{0}.{1}".format(self._task_pack.__name__,
                                                            "seq_test")) # flake8: noqa F841
        test_group = "get_{0}_tests".format(test_group)
        tg_func = getattr(test_seq, test_group)
        return tg_func()

    def check_prereqs(self, task):
        """
        Check if prereq tasks have been completed for a task

        Args:
            task (Task): the task to check

        """

        prereqs_needed = list()
        failed_prereqs = list()

        prereqs = task.task_prereqs
        logger.debug("%s prereqs: %s", task.task_name, prereqs)
        # Todo (BF) Clean this up
        for prereq in prereqs:
            prereq_run = False
            prereq_pass = False
            for c_test, result in self._completed_tasks.iteritems():
                logger.debug("prereq compare: %s prereq "
                             " to complete task %s", prereq, c_test)
                if ((prereq == c_test) or
                        (("{0}.{1}".format(self._task_pack.__name__,
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

    def run_setup_que(self):
        """
        run all tasks in setup que

        """

        self._active_que = self._setup_que
        if not self._claim_per_task:

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
                        raise ResourceUnavailable("Unable to claim resource {0}".format(resource.resource_name))
                except (ResourceRetryExceeded, ResourceUnavailable):
                    self._release_all_resources()
                    raise ResourceUnavailable("Unable to claim resource {0}".format(resource.resource_name))
        self._exec_active_que()

    def run_test_que(self):
        """
        run all tasks in test que

        """

        self._active_que = self._test_que
        self._exec_active_que()

    def run_teardown_que(self):
        """
        run all tasks in the teardown que

        """

        self._active_que = self._teardown_que
        self._exec_active_que()
        if not self._claim_per_task:
            for resource in self.all_resources:
                resource.release_reservation()

    def _reserve_all_resources(self):
        """
        Reserve all resources

        """

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
                raise ResourceUnavailable("unable to reserve resource: {0}".format(unavailable_resource))

    def _release_all_resources(self):
        """
        release all reserved resources

        """
        for resource in self.all_resources:
            resource.release_reservation()

    def _delay_que_add_task(self, task):
        """
        place task and reservation(s) in delay que ordered on reservation time

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
        """
        If a task scheduler/optimizer function has
        been regestered, call the function on the
        active que.

        """

        if self.schedule_func is not None:
            self.schedule_func(self._active_que)

    def _exec_active_que(self):
        """
        execute all tasks in the active que

        """

        if self._use_scheduler:
            self._call_scheduler()


        while True:
            result = dict()

            if self._delay_que:
                if self._delay_que[0]['expire_time'] >= time.time():
                    try:
                        task_to_delay = self._delay_que.pop(0)
                        logger.warning("Delaying task %s due to resource reservation(s) expire",
                                       task_to_delay.task_name)

                        if task_to_delay.add_resource_delay():
                            self._delay_que_add_task(task_to_delay)
                        else:
                            self._release_all_resources()
                            raise ResourceUnavailable

                    except (ResourceRetryExceeded, ResourceUnavailable):
                        warn_msg = ("Skipping test {0}, "
                                    "unable to reserve resources").format(
                                        task_to_delay.task_name)

                        logger.warning(warn_msg)

                        result['task_result'] = "Skipped"
                        result['task_metadata'] = {'message': warn_msg}
                        self._record_result(task_to_delay.task_name, result)

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
                    # if we are here, something went wrong when the task was added to the que
                    self._release_all_resources()
                    raise KissATSError("Fatal prereq error")
                if failed_prereqs:
                    warn_msg = ("Skipping test {0}, "
                                "pre-req(s) {1} failed").format(task_to_run.task_name,
                                                                failed_prereqs)
                    logger.warning(warn_msg)

                    result['task_result'] = "Skipped"
                    result['task_metadata'] = {'message': warn_msg}
                    self._record_result(task_to_run, result)
                    continue


            # reserve per task resources here, if task has a pre-reservation ID, pass that to
            # reservation system

            # if resource won't be available till later place in resource_delay_que

            # if resource isn't available, place at bottom of que
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
                if (task_to_run.task_params['stop_suite_on_fail'] or
                        ("setup" in task_to_run.task_name)):

                    err_msg = ("Critical task {0} failed, "
                               "testing terminated").format(task_to_run.task_name)
                    logger.error(err_msg)
                    self._release_all_resources()
                    raise CriticalTaskFail(err_msg)
            if self._delay_que:
                # if in resource available window, if window has passed, clear pre-reservation ID,
                # place back in active_que
                pass
