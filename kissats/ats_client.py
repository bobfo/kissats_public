"""The base ATS Client class(es) for KISS ATS"""

import abc
import logging
import time

import six

from kissats.exceptions import ServerCommandMissing
from kissats.exceptions import ResourceUnavailable


logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())


@six.add_metaclass(abc.ABCMeta)
class BaseATSClient(object):
    """Base ATS Client Class"""

    def __init__(self):

        super(BaseATSClient, self).__init__()

        self._connected = False
        self._ats_server = None

    @property
    def ats_server(self):
        """The address of the ATS Server"""

        return self._ats_server

    @ats_server.setter
    def ats_server(self, address):

        self._ats_server = address

    @abc.abstractmethod
    def _connect_server(self):
        # type: () -> bool
        """Connect the ATS client to the ATS server"""

        self._connected = True

        return True

    @abc.abstractmethod
    def _disconnect_server(self):
        # type: () -> bool
        """Disconnect the ATS client from the ATS server"""

        self._connected = False

        return True

    @abc.abstractmethod
    def _send_server_command(self, command, extra_data):
        # type: (str, dict) -> str
        """Send a command to the ATS server

        This function is responsible for flattening the command
        and data before transmission to the server

        Args:
            command (str): the command to be sent
            extra_data (dict): any supplemental data

        Returns:
            (str): a unique ID of the command sent

        """

        if not self._connected:
            self._connect_server()

        return "request_ID"

    @abc.abstractmethod
    def _get_server_reply(self, request_id):
        # type: (str) -> dict
        """Get the reply from a server command

        This function is responsible for converting the reply
        from the server to a dict.

        Args:
            request_id (object): the unique ID of the request awaiting reply

        Returns:
            (dict): Dictionary of unflattened reply

        """

        reply_dict = dict()

        return reply_dict

    def server_communicate(self, server_request):
        # type: (dict) -> dict
        """Send a command to the server and return the server reply

        Args:
            server_request (dict): the request to be sent with
                                   a key "command", all other keys
                                   will be placed in the extra data

        Returns:
            (dict): Dictionary of unflattened reply

        Raises:
            ServerCommandMissing:

        """

        try:
            command = server_request.pop("command")
        except KeyError as err:
            raise ServerCommandMissing(err)

        request_id = self._send_server_command(command,
                                               server_request)

        return self._get_server_reply(request_id)

    def get_all_resources(self):
        # type: () -> list
        """Get a list of all resources managed by the ATS

        Returns:
            (list): List of all resources managed by the ATS

        """

        server_request = {"command": "get_all_resources"}

        server_reply = self.server_communicate(server_request)

        return server_reply['resource_list']

    def get_available_resources(self):
        # type: () -> list
        """Get a list of available resources

        Returns:
            (list): List of available resources

        """

        server_request = {"command": "get_available_resources"}

        server_reply = self.server_communicate(server_request)

        return server_reply['resource_list']

    def get_resource_availablity(self, resource, start_time=None,
                                 end_time=None):
        # type: (str, float, float) -> dict
        """Get the time when a resource will become available.

        If the resource is not available at the time requested,
        avail_start and avail_end will be the soonest time slot
        available.

        Args:
            resource (str): name of resource
            start_time (float): Epoch time in seconds
            end_time (float): Epoch time in seconds

        Returns:
            (dict): min keys:
                        * available (bool) True if available at the time requested
                        * avail_start (float)
                        * avail_end (float)

        """

        if start_time is None:
            start_time = time.time()
        if end_time is None:
            end_time = start_time + 3600

        server_request = {"command": "check_available",
                          "resource": resource,
                          "start_time": start_time,
                          "end_time": end_time}

        server_reply = self.server_communicate(server_request)

        return server_reply

    def get_resource_config(self, resource):
        # type: (str) -> object
        """Get the current configuration of a resource

        Args:
            resource (str): name of resource

        Returns:
            (object): the current configuration of the resource

        """

        server_request = {"command": "get_config",
                          "resource": resource}

        server_reply = self.server_communicate(server_request)

        return server_reply

    def request_reservation(self,
                            resource,                     # type: str
                            res_config=None,              # type: Optional[object]
                            time_needed=None,             # type: Optional[float]
                            reservation_duration=3600.0,  # type: Optional[float]
                            next_available=True,          # type: Optional[bool]
                            reservation_mode="exclusive"  # type: Optional[str]
                            ):
        # type: (...) -> tuple[str, float, float]
        """Request resource reservation with an optional configuration.

        this will put a preliminary lock on the resource, the final lock must
        be requested after the time_available using claim_reservation

        Args:
            resource (str): The name of the resource requested
            res_config (object): An object that can be serialized
                                 for transmission to the server.
                                 This optional object will define the requested
                                 configuration.
            time_needed (float): time.time time the resource is needed.
                                 if not provided, default is now
            reservation_duration (float): seconds the resource is requested for
                                          defaults to 3600 (1 hour)
            next_available (bool): If the requested time_needed is not available,
                                   request the next available time.
            reservation_mode (str): "exclusive" or "shared", default "exclusive"

        Returns:
            (tuple):

                * (str): UUID of pre-reservation.
                * (float): epoch time resource will be available
                  with the requested configuration.

                * (float): epoch time the pre_reservation_ID will expire.

        Raises:
            ResourceUnavailable:

        """

        if time_needed is None:
            time_needed = time.time()

        server_request = {"command": "request_reservation",
                          "resource": "{0}".format(resource),
                          "time_needed": time_needed,
                          "reservation_duration": reservation_duration,
                          "configuration": res_config,
                          "reservation_mode": reservation_mode,
                          "resource_config": res_config,
                          "next_available": next_available}

        req_reply = self.server_communicate(server_request)

        if req_reply.get('pre_reservation_id') is None:
            raise ResourceUnavailable("unable to reserve resource")

        return (req_reply['pre_reservation_id'],
                req_reply['time_available'],
                req_reply['expire_time'])

    def claim_reservation(self, pre_reservation_id):
        # type: (str) -> dict
        """Claim a pre-reserved resource.

        * If the reservation is claimed late and will not be available for the
          entire reservation_duration an exception will be raised.

        * If claimed late and another slot is available
          a new pre-reservation will be provided, see Returns below

        Args:
            pre_reservation_id (str): The pre_reservation_id
                                      returned by get_future_reservation

        Returns:
            (dict):

                * Min keys if success
                    * reservation_id(str): if the reservation is a success.
                    * expire_time(float): epoch time when the reservation expires
                    * resource_config(object): Current configuration of the resource

                * Min keys if failure
                    * pre_reservation_id (str): new pre_reservation ID
                    * new_avail (float): New available time
                    * new_expire (float): New expiration time

        Raises:
            ResourceUnavailable:

        """

        server_request = {"command": "claim_reservation",
                          "pre_reservation_id": pre_reservation_id}

        server_reply = self.server_communicate(server_request)

        if (server_reply.get('reservation_ID') is None and
                server_reply.get('pre_reservation_id') is None):

            raise ResourceUnavailable("unable to claim reservation {0}".format(pre_reservation_id))

        return server_reply

    def release_resource(self, reservation_id):
        # type: (str) -> bool
        """Release a previously reserved resource

        Args:
            reservation_id (str): reservation_id or pre-reservation_id
                                  of resource

        Returns:
            (bool):

        """

        server_request = {"command": "release_reservation",
                          "reservation_id": reservation_id}

        server_reply = self.server_communicate(server_request)

        return server_reply.get('release_status', False)
