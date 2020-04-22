from abc import ABC, abstractmethod
from enum import Enum
from typing import Dict
from typing import Sequence
from typing import Union
from typing import Any
from xml.dom import minidom

from procset import ProcSet
import zmq

from .jobs import Job
from .jobs import JobState
from .jobs import JobProfile
from .jobs import DelayJobProfile
from .jobs import ParallelJobProfile
from .jobs import ParallelHomogeneousJobProfile
from .jobs import ParallelHomogeneousTotalJobProfile
from .jobs import ComposedJobProfile
from .jobs import ParallelHomogeneousPFSJobProfile
from .jobs import DataStagingJobProfile
from .resources import PowerState
from .resources import PowerStateType
from .resources import Host
from .resources import Platform


class BatsimEventType(Enum):
    """ Batsim Event types.

    This class enumerates all events the Batsim can dispatch.
    """
    SIMULATION_BEGINS = 0
    SIMULATION_ENDS = 1
    JOB_SUBMITTED = 2
    JOB_COMPLETED = 3
    JOB_STARTED = 4
    JOB_KILLED = 5
    REQUESTED_CALL = 6
    NOTIFY = 7
    RESOURCE_STATE_CHANGED = 8

    def __str__(self) -> str:
        return self.name


class BatsimRequestType(Enum):
    """ Batsim Request types.

    This class enumerates all requests that can be sent to Batsim.
    """
    REJECT_JOB = 0
    EXECUTE_JOB = 1
    CALL_ME_LATER = 2
    KILL_JOB = 3
    REGISTER_JOB = 4
    REGISTER_PROFILE = 5
    SET_RESOURCE_STATE = 6
    SET_JOB_METADATA = 7
    CHANGE_JOB_STATE = 8

    def __str__(self) -> str:
        return self.name


class BatsimNotifyType(Enum):
    """ Batsim Notify types.

    This class enumerates all notifications that can be received or sent.
    """
    NO_MORE_STATIC_JOB_TO_SUBMIT = 0
    NO_MORE_EXTERNAL_EVENT_TO_OCCUR = 1
    REGISTRATION_FINISHED = 2
    CONTINUE_REGISTRATION = 3

    def __str__(self) -> str:
        return self.name


class BatsimRequest(ABC):
    """ Batsim request base class.

    Args:
        timestamp: the time the request will be sent.
        request_type: the type of the request.

    Raises:
        TypeError: In case of invalid arguments.
    """

    def __init__(self, timestamp: float, request_type: BatsimRequestType) -> None:
        if not isinstance(request_type, BatsimRequestType):
            raise TypeError('Expected `request_type` argument to be an '
                            'instance of `BatsimRequestType`, '
                            'got {}.'.format(request_type))

        self.__timestamp = float(timestamp)
        self.__type = request_type

    @property
    def timestamp(self) -> float:
        """ The time the request ocurred. """
        return self.__timestamp

    @property
    def type(self) -> BatsimRequestType:
        """ The request type. """
        return self.__type

    @abstractmethod
    def _get_data_dict(self) -> dict:
        """ Batsim data dict.

        Returns:
            A dict with the properties following the Batsim format.
        """
        raise NotImplementedError

    def _get_extra_params_dict(self) -> dict:
        """ Batsim extra params dict.

        Returns:
            A dict with extra parameters to be added in the request.
        """
        return {}

    def to_json(self) -> dict:
        """ Export properties to the json format expected by Batsim.

        Returns:
            A dict with all parameters following the Batsim format.
        """
        data = self._get_data_dict()
        extra_p = self._get_extra_params_dict()

        params = {
            "timestamp": self.timestamp,
            "type": str(self.type),
            "data": data
        }

        for k, v in extra_p.items():
            params[k] = v

        return params


class BatsimNotify:
    """ Batsim notify class.

    Args:
        timestamp: the time which the notification ocurred or will be sent.
        notify_type: the type of the notification.

    Raises:
        TypeError: In case of invalid arguments.
    """

    def __init__(self, timestamp: float, notify_type: BatsimNotifyType) -> None:
        if not isinstance(notify_type, BatsimNotifyType):
            raise TypeError('Expected `notify_type` argument to be an '
                            'instance of `BatsimNotifyType`, '
                            'got {}.'.format(notify_type))

        self.__timestamp = float(timestamp)
        self.__type = notify_type

    @property
    def timestamp(self) -> float:
        """ The time the notification ocurred. """
        return self.__timestamp

    @property
    def type(self) -> BatsimNotifyType:
        """ The notify type. """
        return self.__type

    def to_json(self) -> dict:
        """ Export properties to the json format expected by Batsim.

        Returns:
            A dict with all parameters following the Batsim format.
        """
        jsn = {
            "timestamp": self.timestamp,
            "type": str(BatsimEventType.NOTIFY),
            "data": {"type": str(self.type).lower()}
        }
        return jsn


class BatsimEvent(ABC):
    """ Batsim event base class.

    Args:
        timestamp: the time which the event occurred.
        event_type: the type of the event.

    Raises:
        TypeError: In case of invalid arguments.
    """

    def __init__(self, timestamp: float, event_type: BatsimEventType) -> None:
        if not isinstance(event_type, BatsimEventType):
            raise TypeError('Expected `event_type` argument to be an '
                            'instance of `BatsimEventType`, '
                            'got {}.'.format(event_type))

        self.__timestamp = float(timestamp)
        self.__type = event_type

    @property
    def timestamp(self) -> float:
        """ The time the event ocurred. """
        return self.__timestamp

    @property
    def type(self) -> BatsimEventType:
        """ The event type. """
        return self.__type

    def _get_data_dict(self) -> dict:
        """ Batsim data dict. """
        return {}

    def to_json(self) -> dict:
        """ Export properties to the json format expected by Batsim.

        Returns:
            A dict with all parameters following the Batsim format.
        """
        params = {
            "timestamp": self.timestamp,
            "type": str(self.type),
            "data": self._get_data_dict()
        }

        return params


class BatsimMessage:
    """ Batsim Message class.

    Args:
        now: current simulation time.
        batsim_events: a sequence of batsim events/requests/notifications.
    """

    def __init__(self,
                 now: float,
                 batsim_events: Sequence[Union[BatsimRequest, BatsimEvent, BatsimNotify]]) -> None:
        self.__now = float(now)
        # Events timestamps must be in (non-strictly) ascending order.
        self.__events = sorted(batsim_events, key=lambda e: e.timestamp)

    @property
    def now(self) -> float:
        """ The current simulation time. """
        return self.__now

    @property
    def events(self) -> Sequence[Union[BatsimRequest, BatsimEvent, BatsimNotify]]:
        """ The list of events/requests/notifications. """
        return list(self.__events)

    def to_json(self) -> dict:
        """ Converts to the json format expected by Batsim

        Returns:
            A dict with all parameters following the Batsim format.
        """
        jsn = {
            "now": self.now,
            "events": [e.to_json() for e in self.events]
        }
        return jsn


class JobStartedBatsimEvent(BatsimEvent):
    """ Batsim job started event class.

    This event is dispatched by Batsim when a job starts.

    Args:
        timestamp: the time which the event occurred.
        job_id: the job id.
        alloc: the resources allocated for the job.
    """

    def __init__(self, timestamp: float, job_id: str, alloc: str) -> None:
        super().__init__(timestamp, BatsimEventType.JOB_STARTED)
        self.__job_id = str(job_id)
        self.__alloc = list(ProcSet.from_str(alloc))

    @property
    def job_id(self) -> str:
        """ The job id. """
        return self.__job_id

    @property
    def alloc(self) -> Sequence[int]:
        """ The allocated resources id for the job. """
        return self.__alloc


class JobCompletedBatsimEvent(BatsimEvent):
    """ Batsim job completed event class.

    This event is dispatched by Batsim when a job finishes.

    Args:
        timestamp: the time which the event ocurred.
        data: the event data dict following the Batsim format.
    """

    def __init__(self, timestamp: float, data: dict) -> None:
        super().__init__(timestamp, BatsimEventType.JOB_COMPLETED)
        self.__job_id = str(data["job_id"])
        self.__job_state = JobState[data["job_state"]]
        self.__return_code = str(data["return_code"])
        self.__alloc = list(ProcSet.from_str(data["alloc"]))

    @property
    def job_id(self) -> str:
        """ The job id. """
        return self.__job_id

    @property
    def job_state(self) -> JobState:
        """ The last state of the job. """
        return self.__job_state

    @property
    def return_code(self) -> str:
        """ The last state of the job. """
        return self.__return_code

    @property
    def alloc(self) -> Sequence[int]:
        """ The allocated resources id for the job. """
        return self.__alloc


class ResourcePowerStateChangedBatsimEvent(BatsimEvent):
    """ Resource power state changed event class.

    This event is dispatched by Batsim when a resource changes its power state.

    Args:
        timestamp: the time which the event ocurred.
        data: the event data dict following the Batsim format.
    """

    def __init__(self, timestamp: float, data: dict) -> None:
        super().__init__(timestamp, BatsimEventType.RESOURCE_STATE_CHANGED)
        self.__resources = list(ProcSet.from_str(data["resources"]))
        self.__state = int(data["state"])

    @property
    def resources(self) -> Sequence[int]:
        """ The resources id. """
        return self.__resources

    @property
    def state(self) -> int:
        """ The power state id. """
        return self.__state


class JobSubmittedBatsimEvent(BatsimEvent):
    """ Batsim Job Submitted event class.

    This event is dispatched by Batsim when a job is submitted to the system.

    Args:
        timestamp: the time which the event ocurred.
        data: the event data dict following the Batsim format.
    """

    def __init__(self, timestamp: float, data: dict) -> None:
        super().__init__(timestamp, BatsimEventType.JOB_SUBMITTED)
        self.__job = Job(
            name=data["job_id"].split(Job.WORKLOAD_SEPARATOR)[1],
            workload=data["job_id"].split(Job.WORKLOAD_SEPARATOR)[0],
            res=data["job"]["res"],
            profile=self.get_profile(data["job"]["profile"], data["profile"]),
            subtime=timestamp,
            walltime=data["job"].get("walltime", None),
            user=data["job"].get("user", None),
        )

    @property
    def job(self) -> Job:
        """ The job. """
        return self.__job

    @staticmethod
    def get_profile(name: str, data: dict) -> JobProfile:
        """ Get a job profile instance from batsim data dict.

        Args:
            name: the profile name. 
            data: the profile data dict following batsim format.

        Returns:
            A job profile.

        Raises:
            NotImplementedError: In case of not supported job profile type.
        """
        if data['type'] == "delay":
            return DelayJobProfile(name, data['delay'])
        elif data['type'] == "parallel":
            return ParallelJobProfile(name, data['cpu'], data['com'])
        elif data['type'] == "parallel_homogeneous":
            return ParallelHomogeneousJobProfile(name, data['cpu'], data['com'])
        elif data['type'] == "parallel_homogeneous_total":
            return ParallelHomogeneousTotalJobProfile(name, data['cpu'], data['com'])
        else:
            raise NotImplementedError('Job profile type not currently supported, '
                                      'got {}.'.format(data['type']))


class JobKilledBatsimEvent(BatsimEvent):
    """ Batsim Job Killed event class.

    This event is dispatched by Batsim when a job is killed.

    Args:
        timestamp: the time which the event ocurred.
        data: the event data dict following the Batsim format.
    """

    def __init__(self, timestamp: float, data: dict) -> None:
        super().__init__(timestamp, BatsimEventType.JOB_KILLED)
        self.__job_ids = list(data["job_ids"])

    @property
    def job_ids(self) -> Sequence[str]:
        """ The id of all jobs killed. """
        return list(self.__job_ids)


class RequestedCallBatsimEvent(BatsimEvent):
    """ Batsim Requested Call event class.

    This event is dispatched by Batsim as a response from a call me back request.

    Args:
        timestamp: the time which the event ocurred.
    """

    def __init__(self, timestamp: float) -> None:
        super().__init__(timestamp, BatsimEventType.REQUESTED_CALL)


class SimulationBeginsBatsimEvent(BatsimEvent):
    """ Batsim Simulation Begins event class.

    This event is dispatched by Batsim when the simulation begins and includes
    all configuration parameters.

    Args:
        timestamp: the time which the event ocurred.
        data: the event data dict following the Batsim format.
    """

    def __init__(self, timestamp: float, data: dict) -> None:
        super().__init__(timestamp, BatsimEventType.SIMULATION_BEGINS)
        self.platform = SimulationBeginsBatsimEvent.get_platform(data)

    @staticmethod
    def get_power_states(properties: dict) -> Sequence[PowerState]:
        """ Get Power States from Batsim json host properties.

        Args:
            data: the properties of the job in the json received by Batsim.

        Returns:
            A sequence of power states.
        """
        if "watt_per_state" not in properties:
            raise RuntimeError('Expected `watt_per_state` property to be '
                               'defined in the host.')

        watt_per_state = properties["watt_per_state"].split(",")
        sleep_pstates = properties.get("sleep_pstates", None)

        pstates_id = {i: i for i in range(len(watt_per_state))}
        power_states = []
        if sleep_pstates:
            sleep_pstates = list(map(int, sleep_pstates.split(":")))  # values

            ps_id = pstates_id.pop(sleep_pstates[0])
            watt = float(watt_per_state[ps_id].split(":")[0])
            sleep_ps = PowerState(pstate_id=ps_id,
                                  pstate_type=PowerStateType.SLEEP,
                                  watt_idle=watt,
                                  watt_full=watt)

            ps_id = pstates_id.pop(sleep_pstates[1])
            watt = float(watt_per_state[ps_id].split(":")[0])
            switch_off_ps = PowerState(pstate_id=ps_id,
                                       pstate_type=PowerStateType.SWITCHING_OFF,
                                       watt_idle=watt,
                                       watt_full=watt)

            ps_id = pstates_id.pop(sleep_pstates[2])
            watt = float(watt_per_state[ps_id].split(":")[0])
            switch_on_ps = PowerState(pstate_id=ps_id,
                                      pstate_type=PowerStateType.SWITCHING_ON,
                                      watt_idle=watt,
                                      watt_full=watt)

            power_states.append(sleep_ps)
            power_states.append(switch_off_ps)
            power_states.append(switch_on_ps)

        # Get Computation Power States
        for ps_id in pstates_id.values():
            watts = watt_per_state[ps_id].split(":")
            comp_ps = PowerState(pstate_id=ps_id,
                                 pstate_type=PowerStateType.COMPUTATION,
                                 watt_idle=float(watts[0]),
                                 watt_full=float(watts[1]))
            power_states.append(comp_ps)

        return power_states

    @staticmethod
    def get_platform(data: dict) -> Platform:
        """ Get a `Platform` from Batsim json.

        Args:
            data: the Batsim json data.

        Returns:
            A platform with all hosts.
        """
        hosts = []
        for r in data["compute_resources"]:
            pstates = None
            props = r.get("properties", None)
            if props and "watt_per_state" in props:
                pstates = SimulationBeginsBatsimEvent.get_power_states(props)

            hosts.append(
                Host(id=r['id'], name=r['name'], pstates=pstates))

        return Platform(hosts)


class SimulationEndsBatsimEvent(BatsimEvent):
    """ Batsim Simulation Ends event class.

    This event is dispatched by Batsim when the simulation ends.

    Args:
        timestamp: the time which the event ocurred.
    """

    def __init__(self, timestamp: float) -> None:
        super().__init__(timestamp, BatsimEventType.SIMULATION_ENDS)


class RejectJobBatsimRequest(BatsimRequest):
    """ Batsim Reject Job request class.

    This request tells batsim to remove a job from the queue. This job
    will not be scheduled nor accounted.

    Args:
        timestamp: the time which the request should occur.
        job_id: the id of the job to be rejected.

    Raises:
        TypeError: In case of invalid arguments.

    Attributes:
        job_id: The job id to be rejected.
    """

    def __init__(self, timestamp: float, job_id: str) -> None:
        super().__init__(timestamp, BatsimRequestType.REJECT_JOB)
        self.job_id = job_id

    def _get_data_dict(self) -> dict:
        """ Batsim data dict.

        Returns:
            A dict with the properties following the Batsim format.
        """
        return {"job_id": self.job_id}


class ExecuteJobBatsimRequest(BatsimRequest):
    """ Batsim Execute Job request class.

    This request tells batsim to execute a job on the allocated resources.

    Args:
        timestamp: the time which the request should occur.
        job_id: the id of the job to execute.
        alloc: the job allocated resources.

    Raises:
        TypeError: In case of invalid arguments.

    Attributes:
        job_id: The id of the job to execute.
        alloc: the job allocated resources.
    """

    def __init__(self, timestamp: float, job_id: str, alloc: Sequence[int]) -> None:
        super().__init__(timestamp, BatsimRequestType.EXECUTE_JOB)
        self.job_id = job_id
        self.alloc = ProcSet(*alloc)

    def _get_data_dict(self) -> dict:
        """ Batsim data dict.

        Returns:
            A dict with the properties following the Batsim format.
        """
        return {"job_id": self.job_id, "alloc": str(self.alloc)}


class CallMeLaterBatsimRequest(BatsimRequest):
    """ Batsim Call Me Later request class.

    This request tells batsim to setup a callback.

    Args:
        timestamp: the time which the request must occur.
        at: the time to be called back.

    Raises:
        TypeError: In case of invalid arguments.
        ValueError: In case of the callback time is not greater than
            the timestamp.

    Attributes:
        at: The time which batsim must callback.
    """

    def __init__(self, timestamp: float, at: float) -> None:
        if timestamp >= at:
            raise ValueError('Expected `timestamp` argument to be less than the '
                             '`at` argument, got `timestamp` = {} '
                             'and `at` = {}.'.format(timestamp, at))

        super().__init__(timestamp, BatsimRequestType.CALL_ME_LATER)
        self.at = float(at)

    def _get_data_dict(self) -> dict:
        """ Batsim data dict.

        Returns:
            A dict with the properties following the Batsim format.
        """
        return {"timestamp": self.at}


class KillJobBatsimRequest(BatsimRequest):
    """ Batsim Kill Job request class.

    This request tells batsim to kill a job. This job can be running or
    waiting in the queue. This is different from the reject request in that
    a job cannot be rejected when it's running.

    Args:
        timestamp: the time which the request should occur.
        *job_id: Variable length of job ids.

    Raises:
        TypeError: In case of invalid arguments.

    Attributes:
        job_ids: A sequence of ids of the jobs to be killed.
    """

    def __init__(self, timestamp: float, *job_id: str) -> None:
        super().__init__(timestamp, BatsimRequestType.KILL_JOB)
        self.job_ids = list(job_id)

    def _get_data_dict(self) -> dict:
        """ Batsim data dict.

        Returns:
            A dict with the properties following the Batsim format.
        """
        return {"job_ids": self.job_ids}


class RegisterJobBatsimRequest(BatsimRequest):
    """ Batsim Register Job request class.

    This request tells batsim to register a job. Only registered jobs can be
    scheduled. This is useful when a local job submitter is desired. By default,
    Batsim submits the jobs defined in the workload and there is no need to
    manually register a job.

    Args:
        timestamp: the time which the request should occur.
        job: The job to be registered.

    Raises:
        TypeError: In case of invalid arguments.

    Attributes:
        job: The registered job.
    """

    def __init__(self, timestamp: float, job: Job) -> None:
        super().__init__(timestamp, BatsimRequestType.REGISTER_JOB)
        if not isinstance(job, Job):
            raise TypeError('Expected `job` argument to be an '
                            'instance of `Job`, got {}.'.format(job))
        self.job = job

    def _get_data_dict(self) -> dict:
        """ Batsim data dict.

        Returns:
            A dict with the properties following the Batsim format.
        """
        job = {
            "profile": self.job.profile.name,
            "res": self.job.res,
            "subtime": self.job.subtime,
            "id": self.job.id,
            "user": self.job.user
        }
        if self.job.walltime:
            job['walltime'] = self.job.walltime

        return {"job_id": self.job.id, "job": job}


class RegisterProfileBatsimRequest(BatsimRequest):
    """ Batsim Register Profile request class.

    This request tells batsim to register a job profile. A job can only be
    registered if its profile is also registered.

    Args:
        timestamp: the time which the request should occur.
        workload_name: The workload name. This is important because job profiles
            with the same name but with different behaviors can coexist. Thus, the
            workload name acts in the same way as a namespace.
        profile: The job profile.

    Raises:
        TypeError: In case of invalid arguments.

    Attributes:
        workload_name: The profile workload name.
        profile: The registered profile.
    """

    def __init__(self, timestamp: float, workload_name: str, profile: JobProfile) -> None:
        super().__init__(timestamp, BatsimRequestType.REGISTER_PROFILE)
        if not isinstance(profile, JobProfile):
            raise TypeError('Expected `profile` argument to be a '
                            '`JobProfile` instance, got {}.'.format(profile))

        self.workload_name = str(workload_name)
        self.profile = profile

    def __get_profile_params(self) -> Dict[str, Any]:
        """ Convert a job profile to the batsim format. """
        params: Dict[str, Any] = {}
        if isinstance(self.profile, DelayJobProfile):
            params['delay'] = self.profile.delay
            params['type'] = 'delay'
        elif isinstance(self.profile, ParallelJobProfile):
            params['cpu'] = self.profile.cpu
            params['com'] = self.profile.cpu
            params['type'] = 'parallel'
        elif isinstance(self.profile, ParallelHomogeneousJobProfile):
            params['cpu'] = self.profile.cpu
            params['com'] = self.profile.cpu
            params['type'] = 'parallel_homogeneous'
        elif isinstance(self.profile, ParallelHomogeneousTotalJobProfile):
            params['cpu'] = self.profile.cpu
            params['com'] = self.profile.cpu
            params['type'] = 'parallel_homogeneous_total'
        elif isinstance(self.profile, ComposedJobProfile):
            params['repeat'] = self.profile.repeat
            params['seq'] = [p.name for p in self.profile.profiles]
            params['type'] = "composed"
        elif isinstance(self.profile, ParallelHomogeneousPFSJobProfile):
            params['bytes_to_read'] = self.profile.bytes_to_read
            params['bytes_to_write'] = self.profile.bytes_to_write
            params['storage'] = self.profile.storage
            params['type'] = "parallel_homogeneous_pfs"
        elif isinstance(self.profile, DataStagingJobProfile):
            params['nb_bytes'] = self.profile.nb_bytes
            params['from'] = self.profile.src
            params['to'] = self.profile.dest
            params['type'] = "data_staging"
        else:
            raise NotImplementedError('Profile not supported, '
                                      'got {}.'.format(self.profile))

        return params

    def _get_data_dict(self) -> dict:
        """ Batsim data dict. 

        Returns:
            A dict with the properties following the Batsim format.
        """
        data = {
            "workload_name": self.workload_name,
            "profile_name": self.profile.name,
            "profile": self.__get_profile_params()
        }
        return data


class SetResourceStateBatsimRequest(BatsimRequest):
    """ Batsim Set Resource State request class.

    This request tells batsim to change the power state of a resource. 

    Args:
        timestamp: The time which the request should occur.
        resources: A sequence of resources id.
        state_id: The power state id.

    Raises:
        ValueError: In case of invalid sequence size.
        TypeError: In case of invalid arguments.

    Attributes:
        resources: A set of resources id.
        state: The resource power state id to switch.
    """

    def __init__(self,
                 timestamp: float,
                 resources: Sequence[int],
                 state_id: int) -> None:

        if not resources:
            raise ValueError('Expected `resources` argument to have at least '
                             'one resource, got {}.'.format(resources))

        super().__init__(timestamp, BatsimRequestType.SET_RESOURCE_STATE)
        self.resources = ProcSet(*resources)
        self.state = state_id

    def add_resource(self, resource_id: int) -> None:
        """ Add a resource id to the request.

        Args:
            resource_id: The resource id.
        """
        self.resources.update(ProcSet(resource_id))

    def _get_data_dict(self) -> dict:
        """ Batsim data dict. 

        Returns:
            A dict with the properties following the Batsim format.
        """
        return {"resources": str(self.resources), "state": str(self.state)}


class ChangeJobStateBatsimRequest(BatsimRequest):
    """ Batsim Change Job State request class.

    This request tells batsim to change the state of a job. 

    Args:
        timestamp: the time which the request should occur.
        job_id: The job id.
        job_state: The job state.
        kill_reason: The motivation to change the job state.

    Raises:
        TypeError: In case of invalid arguments.

    Attributes:
        job_id: The id of the job.
        job_state: The job state to set.
        kill_reason: The motivation to change the job state.
    """

    def __init__(self,
                 timestamp: float,
                 job_id: str,
                 job_state: JobState,
                 kill_reason: str) -> None:

        super().__init__(timestamp, BatsimRequestType.CHANGE_JOB_STATE)
        if not isinstance(job_state, JobState):
            raise TypeError('Expected `job_state` argument to be a '
                            '`JobState` instance, got {}.'.format(job_state))

        self.job_id = str(job_id)
        self.job_state = job_state
        self.kill_reason = str(kill_reason)

    def _get_data_dict(self) -> dict:
        """ Batsim data dict. 

        Returns:
            A dict with the properties following the Batsim format.
        """
        d = {
            "job_id": self.job_id,
            "job_state": self.job_state,
            "kill_reason": self.kill_reason,
        }
        return d


class BatsimMessageDecoder:
    """ Decodes a Batsim message.

    Args:
        msg: The raw Batsim message.

    Returns:
        A platform instance.

    Raises:
        NotImplementedError: In case of not supported simulation config.
    """

    def __call__(self, msg: dict) -> Any:
        if "type" in msg and msg["type"] in list(map(str, BatsimEventType)):
            event_type = BatsimEventType[msg["type"]]
            if event_type == BatsimEventType.JOB_SUBMITTED:
                return JobSubmittedBatsimEvent(msg["timestamp"], msg["data"])
            elif event_type == BatsimEventType.JOB_COMPLETED:
                return JobCompletedBatsimEvent(msg["timestamp"], msg["data"])
            elif event_type == BatsimEventType.JOB_KILLED:
                return JobKilledBatsimEvent(msg["timestamp"], msg["data"])
            elif event_type == BatsimEventType.RESOURCE_STATE_CHANGED:
                return ResourcePowerStateChangedBatsimEvent(
                    msg["timestamp"],
                    msg["data"])
            elif event_type == BatsimEventType.REQUESTED_CALL:
                return RequestedCallBatsimEvent(msg["timestamp"])
            elif event_type == BatsimEventType.SIMULATION_BEGINS:
                allow_sharing = msg["data"].get("allow_compute_sharing", False)
                if allow_sharing:
                    raise NotImplementedError('Sharing resources is currently '
                                              'not supported.')
                return SimulationBeginsBatsimEvent(msg["timestamp"], msg["data"])
            elif event_type == BatsimEventType.SIMULATION_ENDS:
                return SimulationEndsBatsimEvent(msg["timestamp"])
            elif event_type == BatsimEventType.NOTIFY:
                return BatsimNotify(
                    msg["timestamp"],
                    BatsimNotifyType[msg["data"]["type"].upper()])
            else:
                return msg
        elif "now" in msg:
            return BatsimMessage(msg["now"], msg["events"])
        else:
            return msg


class NetworkHandler:
    """ Batsim Network Handler class.

    This class handles the network connection with Batsim.

    Args:
        address: An address string consisting of three parts  as follows: 
            protocol://interface:port.

    Examples:
        >>> net = NetworkHandler(address="tcp://localhost:28000")
    """

    def __init__(self, address: str) -> None:
        self.__address = str(address)
        self.__context = zmq.Context()
        self.__socket: zmq.Socket = None

    @property
    def address(self) -> str:
        """ The address. """
        return self.__address

    def bind(self) -> None:
        """ Create and bind a socket to the address. 

        Raises:
            SystemError: In case of a connection is already opened.
        """
        if self.__socket:
            raise SystemError("Connection already opened.")

        self.__socket = self.__context.socket(zmq.REP)
        self.__socket.bind(self.address)

    def close(self) -> None:
        """ Close the socket. """
        if self.__socket:
            self.__socket.close()
            self.__socket = None

    def send(self, msg: BatsimMessage) -> None:
        """ Send a message.

        Args:
            msg: The message to be sent.

        Raises:
            TypeError: In case of invalid argument type.
            SystemError: In case of there is no connection opened.
        """
        if not isinstance(msg, BatsimMessage):
            raise TypeError('Expected `msg` argument to be a '
                            '`BatsimMessage` instance, got {}.'.format(msg))

        if not self.__socket:
            raise SystemError("Connection not opened.")

        self.__socket.send_json(msg.to_json())

    def recv(self) -> BatsimMessage:
        """ Receive a message. 

        The default behavior is to wait until a message arrives.

        Raises:
            SystemError: In case of there is no connection opened.
        """
        if not self.__socket:
            raise SystemError("Connection not opened.")

        return self.__socket.recv_json(object_hook=BatsimMessageDecoder())

    def send_and_recv(self, msg: BatsimMessage) -> BatsimMessage:
        """ Send a message and wait for a response.

        Args:
            msg: The message to be sent.

        Raises:
            TypeError: In case of invalid argument type.
            SystemError: In case of there is no connection opened.
        """
        self.send(msg)
        return self.recv()
