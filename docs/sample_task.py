"""
A sample task for Kiss ATS


"""
# TODO(BF): Needs updating

import logging


logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())


def get_params(global_params):
    """"
    The paramaters of the task

    These params will also be passed to the ats_client class

    An example implementation:
        If the task requires a 32-bit Linux resource and the ATS
        manager has the capibility to configure resources some of
        the params might look like:
             params['exclusive_resources'] = ['linux_pc']
             params['resource_config'] = {'linux_pc':['32-bit', 'centOS']}

        All params will be flattened and sent to the ATS manager via the
        defined ATS client.  The ATS manager would schedule the configuration
        of a resource with a 32 bit instalation of centOS.  The ATS manager
        would return a pre-reservaton ID and a time when the resource will
        be ready.  The task_pack will delay the test/task until the resource
        is ready and continue with other test/task actons.

    required params keys:
        * name
        * description

    all other keys are optional, see kissats.task.Task.task_params for defaults

    """

    params = dict()
    params['name'] = __file__
    params['description'] = __doc__
    params['stop_suite_on_fail'] = False  # if this task fails, stop all further testsing # noqa: E501
    params['exclusive_resources'] = list()
    params['shared_resources'] = list()
    params['max_resource_wait'] = int()  # max time to wait for a resource to be available in seconds # noqa: E501
    params['max_resource_retry'] = int()  # if resources are busy, max times to retry # noqa: E501
    params['thread_safe'] = False
    params['process_safe'] = False
    params['valid_ats'] = list()
    params['valid_duts'] = list()
    params['req_param_keys'] = list()  # keys required to be present in the global parameter dictionary # noqa: E501
    params['optional_param_keys'] = list()  # optional keys that will be used if present in the global parameter dictionary # noqa: E501
    params['prereq_tasks'] = list()
    params['est_test_time'] = int()  # estimated test time in seconds (including optional setup and teardown) # noqa: E501
    params['always_teardown'] = False

    return params


def task_setup(global_params):
    """
    Setup action for this task.

    required return value is None

    If function has a condition that needs to stop testing or
    the run of the task an exception must be raised.

    Warning:
        Setup actions are NOT tests and should not test.
        Setup conditions should be verified before returning
        from the function and if not met an exception should
        be raised to halt testing.

    """

    return


def run(global_params):
    """
    The Task itself

    If the valid_ats is a valid kiss ats available on pypi or
    already installed, the config will contain an instantiated client
    with all needed resources reserved, once the task is completed the
    resources will be released

    required return is a dict containing at least
    the "task_result" and "task_metadata" keys

    an optional additional key 'multi_result' is permited
    multi_result must be a list of dicts containing the "name",
    "description", "task_result" and "task_metadata" keys.
    The items in the list will be reported in the order they
    are contained in the list

    """

    task_result = "Passed"
    task_message = ""

    # Multi result is an optional return dictionary item
    multi_result = list()

    multi_result.append({'name': "sub_task", 'description': "sub_description",
                         'task_result': "sub_task_result", 'task_metadata': "sub_task_message"})

    return {'task_result': task_result, 'task_metadata': task_message, 'multi_result': multi_result}


def test_teardown(global_params):
    """
    Setup action for this task.

    required return value is None

    If function has a condition that needs to stop testing or
    the run of the task an exception must be raised.

    Warning:
        Teardown actions are NOT tests and should not test.
        Post test conditions should be verified before returning
        from the function and if not met an exception should
        be raised to halt testing.

    """

    return
