"""
Base resource class

"""


import logging
import time

from kissats import ResourceUnavailable
from kissats import ResourceRetryExceeded
from kissats import InvalidConfigRequest


logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())


class ResourceReservation(object):
    """
    An ATS Resource ...

    Args:
        resouce_name(str): The name of the resource
        ats_client(BaseATSClient): ATS client class for
                                    communication to the ATS reservation system
        mode(str): "exclusive" or "shared", default "exclusive"
        max_retry(int): Max number of time to attempt to reserve the resource
                        before raising an exception
        max_wait(float): Max time to wait for the resource to become available
                            before raising an exception

    """

    def __init__(self, resource_name, ats_client, mode="exclusive",
                 max_retry=5, max_wait=None):
        # type: (str, BaseATSClient, str, int, float) -> None

        super(ResourceReservation, self).__init__()

        self._resource_name = resource_name
        self._max_retry = max_retry
        self._reservation_id = None
        self._pre_reservation_id = None
        self._pre_res_expire = None
        self._start_time = None
        self._end_time = None
        self._max_wait_time = max_wait
        self._renew_count = 0
        self._retry_count = 0
        self._first_request_time = None
        self._ats_client = ats_client
        self._requested_config = None
        self._returned_config = None
        self._reservation_mode = mode

    @property
    def resource_name(self):
        """
        Name of the resource

        """
        return self._resource_name

    @property
    def resource_config(self):
        """
        * If the resource has not been reserverd, the configuration
          to request.

        * If the resource is reserved the actual
          configuration returned by the ATS

        """

        if self._reservation_id is not None:
            active_config = self._returned_config
        else:
            active_config = self._requested_config

        return active_config

    @resource_config.setter
    def resource_config(self, config):
        if self._reservation_id is None and self._pre_reservation_id is None:
            self._requested_config = config
        else:
            raise InvalidConfigRequest("Can not change config "
                                       "of a reservered resource")

    @property
    def reservation_mode(self):
        """
        Reservation mode. IE: exclusive or shared

        """

        return self._reservation_mode

    @reservation_mode.setter
    def reservation_mode(self, mode):

        self._reservation_mode = mode

    @property
    def max_retry(self):
        # type: ()-> int
        """
        Max number of times to attempt to reserve the resource

        """
        return self._max_retry

    @property
    def max_wait_time(self):
        # type: ()-> float
        """
        Max amount of time to wait for the resource to become
        available
        Warning:
            If set to None, will wait indefinitely

        """

        return self._max_wait_time

    @max_wait_time.setter
    def max_wait_time(self, wait_time):
        self._max_wait_time = wait_time

    @property
    def reservation_id(self):
        # type: ()-> str
        """
        ID of the currently claimed reservation
        if not currently reserved, value is None

        """

        return self._reservation_id

    @property
    def pre_reservation_id(self):
        # type: ()-> str
        """
        If reservation is claimed, value is None
        If no reservation has been requested, value is None

        """

        return self._pre_reservation_id

    @property
    def pre_res_expire(self):
        # type: ()-> float
        """
        experation time of the pre-reservation_id

        """

        return self._pre_res_expire

    @property
    def start_time(self):
        # type: ()-> float
        """
        Start time of the reservation

        """

        if self._start_time is None:
            self._start_time = time.time()

        return self._start_time

    @property
    def end_time(self):
        # type: ()-> float
        """
        end time of the reservation

        """

        if self._end_time is None:
            self._end_time = self.start_time + 3600
        return self._end_time

    @property
    def retry_count(self):
        # type: ()-> int
        """
        Number of unsuccessful attempts to reserve or claim the resource

        """

        return self._retry_count

    @property
    def first_request_time(self):
        # type: ()-> float
        """
        time of the first request to reserve the resource

        """

        return self._first_request_time

    def _add_retry(self):
        """
        add another retry to the counter
        if retry count exceeds max retry, will raise

        Raises:
            ResourceRetryExceeded

        """

        self._retry_count += 1
        if self._retry_count > self._max_retry:
            raise ResourceRetryExceeded()

    def claim_reservation(self):
        # type: ()-> bool
        """
        Claim reservation

        Returns:
            (bool): True if successful

        """

        claim_reply = self._ats_client.claim_reservation(self.pre_reservation_id)

        if claim_reply.get('reservation_id') is not None:
            self._pre_reservation_id = None
            self._pre_res_expire = None
            self._reservation_id = claim_reply['reservation_id']
            self._end_time = claim_reply['expire_time']
            self.resource_config = claim_reply['resource_config']
            return True
        else:
            self._add_retry()
            self._pre_reservation_id = claim_reply['pre_reservation_id']
            self._start_time = claim_reply['new_avail']
            self._pre_res_expire = claim_reply['new_expire']

        return False

    def release_reservation(self):
        # type: ()-> None
        """
        release the reservation

        """

        if self._pre_reservation_id is not None:
            self._ats_client.release_resource(self._pre_reservation_id)
            self._pre_reservation_id = None
        elif self._reservation_id is not None:
            self._ats_client.release_resource(self._reservation_id)
            self._reservation_id = None

    def request_reservation(self, new_start_time=None, new_end_time=None, next_available=True):
        # type: (Optional[float], Optional[float], Optional[float])-> bool
        """
        request a reservation

        """

        self.release_reservation()
        self._start_time = new_start_time

        self._end_time = new_end_time
        try:
            (self._pre_reservation_id,
             self._start_time,
             self._pre_res_expire) = self._ats_client.get_future_reservation(self.resource_name,
                                                                             self.start_time,
                                                                             self.end_time-self.start_time,  # noqa: E501
                                                                             self.resource_config,
                                                                             next_available)

            if self.first_request_time is None:
                self.first_request_time = self.start_time

        except ResourceUnavailable:
            self._add_retry()
            return False

        return True
