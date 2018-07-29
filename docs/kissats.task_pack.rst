Task Pack
=========

A Task Pack is a class that manages a group of setup tasks,
teardown tasks, tests and resources.

Property definitions
--------------------

Resource Run Modes
++++++++++++++++++

.. Note::
    * The resource mode only applies if an ATS Client
      has been registered.

    * For all custom modes, a scheduler function
      must be regesiterd with schedule_func before
      calling any run que methods.

.. Warning::
    "all" is the only mode currently implemented


*   **all** Will reserve all resources needed
    by all tasks for the estimated duration
    of the run. **This is the default mode**

        .. Note::
            run_all_que method must be used for running tasks


*   **per_task_separate** Will reserve resources
    on a task by task basis as each task is
    run. Each task will hold its own set of
    resources.

*   **per_task_combine** Will reserve resources
    on a task by task basis as each task is
    run. Resources common to tasks
    will be consolidated so they use the same resource.

*   **custom_all** The test que will be passed to the function registered
    with schedule_func for ordering. Resource
    management will be handled the same as
    **all**

*   **custom_separate** The que selected for execution
    will be passed to the function registered
    with schedule_func for ordering. Resource
    management will be handled the same as
    **per_task_separate**

*   **custom_combine** The que selected for execution
    will be passed to the function registered
    with schedule_func for ordering. Resource
    management will be handled the same as
    **per_task_combine**

ats_client
++++++++++

ats_client module containing the ATS_Client
class to import and instantiate for resource
management, if None, no client will be used.

    .. Note::
        if set to "auto", we will attempt to first
        import an ATS client based on the ATS name
        provided in the init_params, if that fails,
        we will attempt to pip install, then
        import, if that fails a KissATSError will
        be raised


TaskPack Class
--------------

.. autoclass:: kissats.task_pack.TaskPack
    :members:
    :undoc-members:
    :inherited-members:
    :show-inheritance:


PackParams Class
----------------

.. autoclass:: kissats.task_pack.PackParams
    :members:
    :undoc-members:
    :inherited-members:
    :show-inheritance:
