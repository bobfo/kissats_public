"""
Task package

"""

from collections import deque
import importlib
import json
import logging
import sys
import time

from kissats.common import pip_in_package
from kissats.task import Task
from kissats import (KissATSError,
                     InvalidTask,
                     FailedPrereq,
                     CriticalTaskFail,
                     ResourceRetryExceeded)


logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())


class TaskPack(object):
    """
    Imports the task package and executes tasks

    Args:
        task_package (string): importable test/task package name
        task_version (string): PIP compatible version string IE: >=1.0.0
        params_input (dict): paramater dict for all tasks
        test_group_in (string): test group to execute
        ats_client (string): ats_client module containing the ATS_Client
                                class to import and instantiate
                                for resource management, if None, no client
                                will be used.
                                Note:
                                    if set to "auto", we will attempt to first
                                    import an ATS client based on the ATS name
                                    provided in the params_input, if that fails,
                                    then we will attempt to pip install, then
                                    import, if that fails a KissATSError will
                                    be raised

    """

    def __init__(self, task_package, task_version=None, params_input=None,
                 test_group_in=None, ats_client=None):

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

        self._test_que = deque()
        self._setup_que = deque()
        self._teardown_que = deque()
        self._delay_que = list()

        self.params = params_input
        self._active_que = self._setup_que
        if test_group_in is not None:
            self.add_test_group(test_group_in)

    @property
    def params(self):
        """
        The parameter dict

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
        the list of setup tasks required by the task package

        """
        if self._setup_list is None:
            setup_seq = importlib.import_module("{0}.{1}".format(self._task_pack.__name__,
                                                                 "seq_setup"))
            self._setup_list = setup_seq.get_global_setup(self.params)
        return self._setup_list

    @property
    def teardown_list(self):
        """
        the list of teardown tasks required by the task package

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
                        raise KissATSError("ATS Client package {0} "
                                           "failed to install".format(pip_str))
                    ats_module = importlib.import_module(ats_client_mod_name)

            else:
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
        report results using a registered reporting function
        if no reporting function is registered, result will be
        reported using the python built in logging module

        Args:
            name(str): The name of the task
            description(str): A short description of the task
            result(str): the result of the task, IE: Passed, Failed, etc.
            metadata(str):  Any test metadata, typicly a flattened format
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
            task(kissats.task.Task): Task to add
            allow_dupe(bool): Allow the task to run multiple times
            top(bool): Place the task at the top of the que


        """

        self._active_que = self._test_que
        self._add_task_to_que(task, allow_dupe, top)

    def add_setup_task(self, task, allow_dupe=False, top=False):
        """
        add a task to the setup que

        """

        self._active_que = self._setup_que
        self._add_task_to_que(task, allow_dupe, top)

    def add_teardown_task(self, task, allow_dupe=False, top=False):
        """
        add a task to the teardown que

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
            task_to_add = self._import_task(task)

        if (task_to_add in self._active_que) and not allow_dupe:
            logger.debug("unable to add %s due to "
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
                    raise FailedPrereq(err_msg)
                return False

            if prereqs_needed:
                for prereq in prereqs_needed:
                    in_prereq = self._import_task(prereq)
                    try:
                        self._active_que.remove(in_prereq)
                        logger.info("%s removed from que", in_prereq.task_name)
                    except ValueError:
                        pass

                    if not self._add_task_to_que(prereq, top=True):
                        raise KissATSError("Critical error adding "
                                           "prereq task to que")

        return True

    def _import_task(self, task):
        """
        Attempt to import a task

        """

        try:
            importlib.import_module(task)
            imported_task = Task(task, self.params, self.ats_client)
        except ImportError:
            try:
                pack_task = "{0}.{1}".format(self._task_pack.__name__,
                                             task)
                importlib.import_module(pack_task)
                imported_task = Task(task, self.params, self.ats_client)
            except ImportError:
                raise InvalidTask("{0} is an invalid task "
                                  "or failed to import".format(task))
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
                                There must be a coresponding get_<test_group>_tests
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
        test_group = "test_seq.get_{0}_tests".format(test_group)
        tg_func = eval(test_group)
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
        if a task scheduler/optimizer function has
        been regestered, call the function.

        """

        if self.schedule_func is not None:
            return self.schedule_func(self._test_que)

        return None

    def _reserve_resources(self, task, start_time=time.time()):
        """
        Reserve all resources for a task.

        """

        # .. Todo::
        #       finish this

    def _exec_active_que(self):
        """
        execute all tasks in the active que

        """

        # call a scheduler function if registered
        # the scheduler is only applied to the test que
        if self._call_scheduler() is None:
            pass

        while True:
            result = dict()
            try:
                if self._delay_que:
                    if self._delay_que[0]['expire_time'] >= time.time():
                        try:
                            self._delay_que[0]['task'].add_resource_delay()

                        except ResourceRetryExceeded:
                            warn_msg = ("Skipping test {0}, "
                                        "unable to reserver resources").format(
                                            self._delay_que[0]['task'].task_name)

                            logger.warning(warn_msg)

                            result['task_result'] = "Skipped"
                            result['task_metadata'] = {'message': warn_msg}
                            self._record_result(self._delay_que[0]['task'], result)
                        finally:
                            self._delay_que.pop(0)
                        continue

                    if self._delay_que[0]['all_resource_ready'] >= time.time():

                        task_to_run = self._delay_que.pop()
                        task_not_ready = False
                        for resource in task_to_run.resource_pre_res_list:
                            try:
                                if not resource.claim_reservation():
                                    task_not_ready = True

                            except (SystemExit, KeyboardInterrupt):
                                raise
                            except Exception, err:
                                logger.exception(err)
                                result['test_status'] = "Exception"
                                result['task_metadata'] = {'task_error': err}
                                self._record_result(task_to_run, result)
                                # task_err = True
                                break
                        if task_not_ready:
                            continue

                else:
                    task_to_run = self._active_que.popleft()
            except IndexError:
                break
            if not self.ignore_prereq:
                prereqs_needed, failed_prereqs = self.check_prereqs(task_to_run)
                if prereqs_needed:
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

            # reserve resources here, if task has a pre-reservation ID, pass that to
            # reservation system

            # if resouce won't be available till later place in resource_delay_que

            # if resource isn't available, place at bottom of que
            try:
                start_time = time.time()
                result = task_to_run.run_task()
                run_time = time.time() - start_time
            finally:
                pass
                # release resources here

            if result['task_metadata'].__class__ is dict:
                result['task_metadata']['run_time'] = run_time

            self._record_result(task_to_run, result)
            if result['task_result'] not in self.valid_task_result:
                if (task_to_run.task_params['stop_suite_on_fail'] or
                        ("setup" in task_to_run.task_name)):

                    err_msg = ("Critical task {0} failed, "
                               "testing terminated").format(task_to_run.task_name)
                    logger.error(err_msg)
                    raise CriticalTaskFail(err_msg)
            if self._delay_que:
                pass

                # if in resource available window, if window has passed, clear pre-reservation ID,
                # place back in active_que
