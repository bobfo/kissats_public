"""A sample task for Kiss ATS"""
# TODO(BF): Needs updating

import logging


logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())


def get_params(global_params):
    """"The parameters for executing the task

    An example implementation:
        If the task requires a 32-bit Linux PC running centOS and the ATS
        manager has the capability to configure resources.
        Some of the params might look like:
             params['exclusive_resources'] = ['linux_pc']
             params['resource_config'] = {'linux_pc':['32-bit', 'centOS']}

        The object contained in the key "linux_pc" will be flattened and sent
        to the ATS manager via the defined ATS client.  The ATS manager would
        schedule the configuration of a resource with a 32 bit installation of
        centOS.  The ATS manager would return a pre-reservation ID and a time
        when the resource will be ready.  The task_pack will delay the test/task
        until the resource is configured/ready.  Depending on the resource_mode
        selected task_pack will continue with other test/task actions while
        waiting or wait for the resource to be ready.

    required params keys:
        * name
        * description

    """

    params = dict()
    # required keys
    params['name'] = __file__
    params['description'] = __doc__
    # optional keys, values listed are defaults
    params['stop_suite_on_fail'] = False
    params['exclusive_resources'] = list()
    params['shared_resources'] = list()
    params['max_resource_wait'] = int()
    params['max_resource_retry'] = int()
    params['thread_safe'] = False
    params['process_safe'] = False
    params['valid_ats'] = list()
    params['valid_duts'] = list()
    params['req_param_keys'] = list()
    params['optional_param_keys'] = list()
    params['prereq_tasks'] = list()
    params['est_test_time'] = int()
    params['always_teardown'] = False
    params['priority'] = 5
    params['extra_metadata'] = None

    return params


def task_setup(global_params):
    """Setup action for this task.

    required return value is None

    If the function encounters a condition that needs to stop all
    testing or task execution an exception must be raised.

    Warning:
        Setup actions are NOT tests and should not test.
        Setup conditions should be verified before returning
        from the function and if not met an exception should
        be raised to halt testing.

    """

    return


def task_main(global_params):
    """The main task function

    If the valid_ats is a valid kiss ats available on pypi or
    already installed, the global_params will contain an instantiated client
    with all needed resources claimed.

    required return is a dict containing at least
    the "result" and "metadata" keys

    An optional additional key 'multi_result' is permitted.
    multi_result must be a list of dictionaries containing the "name",
    "description", "result" and "metadata" keys.
    The items in the list will be reported in the order they
    are contained in the list.

    """

    result = "Passed"
    task_message = ""

    # Multi result is an optional return dictionary item
    multi_result = list()

    multi_result.append({'name': "sub_task", 'description': "sub_description",
                         'result': "sub_result", 'metadata': "sub_task_message"})

    return {'result': result, 'metadata': task_message, 'multi_result': multi_result}


def task_teardown(global_params):
    """Teardown action for this task.

    required return value is None

    If the function encounters a condition that needs to stop all
    testing or task execution an exception must be raised.

    Warning:
        Teardown actions are NOT tests and should not test.
        Post test conditions should be verified before returning
        from the function and if not met an exception should
        be raised to halt testing.

    """

    return
