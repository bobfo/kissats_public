"""Package queue handlers"""


import abc
from collections import deque
import logging
import sys

import six

from kissats.exceptions import KissATSError


logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())


@six.add_metaclass(abc.ABCMeta)
class BaseQues(object):
    """Base class for task management queues"""

    def __init__(self):
        super(BaseQues, self).__init__()

    @property
    @abc.abstractmethod
    def active_que_len(self):
        # type: () -> int
        """length of the active queue"""

    @property
    @abc.abstractmethod
    def delay_que_len(self):
        # type: () -> int
        """length of the delay queue"""

    @abc.abstractmethod
    def set_active_que(self, que_name):
        # type: () -> None
        """Set the active queue

        Args:
            que_name(str): queue to set active

        """

    @abc.abstractmethod
    def clear_active_que(self):
        # type: () -> None
        """Clear the active queue"""

    @abc.abstractmethod
    def clear_all_que(self):
        # type: () -> None
        """clear all queues"""

    @abc.abstractmethod
    def clear_delay_que(self):
        # type: () -> None
        """Clear the delay queue"""

    @abc.abstractmethod
    def in_active_que(self, task):
        # type: (Task) -> bool
        """Check if a task is already in the active queue

        Args:
            task(Task): The task to check

        """

    @abc.abstractmethod
    def remove_from_active_que(self, task):
        # type: (Task) -> None
        """remove a task from the active queue

        Args:
            task(object): task to remove

        """

    @abc.abstractmethod
    def add_to_active_que(self, task, top=False):
        # type: (Task, bool) -> bool
        """add a task to the active queue"""

    @abc.abstractmethod
    def delay_que_add_task(self, task):
        # type: (Task) -> None
        """place task(s) and reservation(s) in delay queue ordered on reservation time

        Args:
            task(Task): Task to add

        """

    @abc.abstractmethod
    def pop_active(self):
        # type: (None) -> Task
        """pop the next item from the right side (bottom)"""

    @abc.abstractmethod
    def popleft_active(self):
        # type: (None) -> Task
        """pop the next item from the left side (top)"""

    @abc.abstractmethod
    def peek_delay(self):
        # type: (None) -> Task
        """peek at the right side (bottom)"""


class PackQues(BaseQues):
    """Task manager queues"""

    def __init__(self):

        super(PackQues, self).__init__()

        # build the queues
        self._test_que = deque()
        self._setup_que = deque()
        self._teardown_que = deque()
        self._active_que = self._setup_que
        # special delay queue
        self._delay_que = list()

        self._que_switch = {
            "test": self._test_que,
            "setup": self._setup_que,
            "teardown": self._teardown_que
            }

    @property
    def active_que_len(self):
        # type: () -> int
        """length of the active queue"""
        return len(self._active_que)

    @property
    def delay_que_len(self):
        # type: () -> int
        """length of the delay queue"""
        return len(self._delay_que)

    def set_active_que(self, que_name):
        """Set the active queue

        Args:
            que_name(str): Queue to set active

        Raises:
            KissATSError: If queue name is invalid

        """

        try:
            self._active_que = self._que_switch[que_name]
        except KeyError:
            raise KissATSError("{0} is not a valid active que".format(que_name))

    def clear_active_que(self):
        """Clear the active queue"""

        self._active_que.clear()

    def clear_all_que(self):
        """clear all queues"""

        self._delay_que = list()
        self._setup_que.clear()
        self._teardown_que.clear()
        self._test_que.clear()

    def clear_delay_que(self):
        """Clear the delay queue"""
        self._delay_que = list()

    def in_active_que(self, task):
        """Check if a task is already in the active queue

        Args:
            task(Task): The task to check

        Returns:
            bool: True if present

        """
        return task in self._active_que

    def remove_from_active_que(self, task):
        """remove a task from the active queue

        Args:
            task(object): task to remove

        """

        self._active_que.remove(task)

    def add_to_active_que(self, task, top=False):
        """add a task to the active queue"""

        if top:
            self._active_que.appendleft(task)
            logger.info("%s added to top of que", task.name)
        else:
            self._active_que.append(task)
            logger.info("%s added to bottom of que", task.name)

        return True

    def delay_que_add_task(self, task):
        """place task(s) and reservation(s) in delay queue ordered on reservation time

        Args:
            task(Task): Task to add

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

    def pop_active(self):
        # type: (None) -> Task
        """pop the next item from the right side (bottom)"""

        return self._active_que.pop()

    def popleft_active(self):
        # type: (None) -> Task
        """pop the next item from the left side (top)"""

        return self._active_que.popleft()

    def pop_delay(self):
        # type: (None) -> Task
        """pop the next item from the right side (bottom)"""

        return self._delay_que.pop()

    def peek_delay(self):
        # type: (None) -> Task
        """peek at the right side (bottom)"""

        return self._delay_que[-1]
