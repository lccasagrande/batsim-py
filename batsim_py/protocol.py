from abc import ABC, abstractmethod
from collections import defaultdict
from enum import Enum
from typing import Optional
from typing import Dict
from typing import Sequence
from typing import Union
from typing import Any

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
from .resources import Storage
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
    JOB_KILLED = 4
    REQUESTED_CALL = 5
    NOTIFY = 6
    RESOURCE_STATE_CHANGED = 7

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
    NOTIFY = 9

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
    EVENT_MACHINE_UNAVAILABLE = 4
    EVENT_MACHINE_AVAILABLE = 5

    def __str__(self) -> str:
        return self.name


class BatsimRequest(ABC):
    """ Batsim request base class.

    Args:
        timestamp: the time the request will be sent.
        request_type: the type of the request.

    Raises:
        AssertionError: In case of invalid arguments type.
    """

    def __init__(self, timestamp: float, request_type: BatsimRequestType) -> None:
        assert isinstance(request_type, BatsimRequestType)
        self.__timestamp = float(timestamp)
        self.__type = request_type

    @property
    def timestamp(self) -> float:
        """ The time the request occurred. """
        return self.__timestamp

    @property
    def type(self) -> BatsimRequestType:
        """ The request type. """
        return self.__type

    @abstractmethod
    def _get_data_dict(self) -> dict:
        """ Batsim data dict.

        Returns:
            A dict with class properties following Batsim format.
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

        params = {
            "timestamp": self.timestamp,
            "type": str(self.type),
            "data": self._get_data_dict()
        }

        extra_params = self._get_extra_params_dict()
        params.update(extra_params)
        return params


class BatsimEvent(ABC):
    """ Batsim event base class.

    Args:
        timestamp: the time which the event occurred.
        event_type: the type of the event.

    Raises:
        AssertionError: In case of invalid arguments.
    """

    def __init__(self, timestamp: float, event_type: BatsimEventType) -> None:
        assert isinstance(event_type, BatsimEventType)
        self.__timestamp = float(timestamp)
        self.__type = event_type

    @property
    def timestamp(self) -> float:
        """ The time the event occurred. """
        return self.__timestamp

    @property
    def type(self) -> BatsimEventType:
        """ The event type. """
        return self.__type


class BatsimMessage:
    """ Batsim Message class.

    Args:
        now: current simulation time.
        batsim_events: a sequence of batsim events and requests.

    Raises:
        ValueError: In case of `now` is not greather than or equal to every
            event timestamp.
    """

    def __init__(self,
                 now: float,
                 batsim_events: Sequence[Union[BatsimRequest, BatsimEvent]] = ()) -> None:

        if any(e.timestamp > now for e in batsim_events):
            raise ValueError('Expected `now` argument to be greather than '
                             'or equal to every event timestamp, got now={} '
                             'and events={}'.format(now, batsim_events))
        self.__now = float(now)
        # Events timestamps must be in (non-strictly) ascending order.
        self.__events = sorted(batsim_events, key=lambda e: e.timestamp)

    @property
    def now(self) -> float:
        """ The current simulation time. """
        return self.__now

    @property
    def events(self) -> Sequence[Union[BatsimRequest, BatsimEvent]]:
        """ The list of events/requests. """
        return list(self.__events)

    def to_json(self) -> dict:
        """ Convert requests to the json format expected by Batsim

        Returns:
            A dict with all parameters following the Batsim format.
        """
        jsn = {
            "now": self.now,
            "events": [
                e.to_json() for e in self.events if isinstance(e, BatsimRequest)
            ]
        }
        return jsn


class NotifyBatsimEvent(BatsimEvent):
    """ Batsim notify event class.

    Args:
        timestamp: the time which the notification occurred.
        data: the event data dict following the Batsim format.

    Raises:
        ValueError: In case of invalid notification type.
    """

    def __init__(self, timestamp: float, data: dict) -> None:
        super().__init__(timestamp, BatsimEventType.NOTIFY)
        self.__notify_type = BatsimNotifyType[data["type"].upper()]
        self.__resources: Optional[Sequence[int]] = None

        external_events = (BatsimNotifyType.EVENT_MACHINE_AVAILABLE,
                           BatsimNotifyType.EVENT_MACHINE_UNAVAILABLE)
        if self.__notify_type in external_events:
            self.__resources = list(ProcSet.from_str(data["resources"]))

    @property
    def notify_type(self) -> BatsimNotifyType:
        """ The notification type. """
        return self.__notify_type

    @property
    def resources(self) -> Optional[Sequence[int]]:
        """ The resources affected by an external event. """
        return self.__resources


class JobCompletedBatsimEvent(BatsimEvent):
    """ Batsim job completed event class.

    This event is dispatched by Batsim when a job finishes.

    Args:
        timestamp: the time which the event occurred.
        data: the event data dict following the Batsim format.
    """

    def __init__(self, timestamp: float, data: dict) -> None:
        super().__init__(timestamp, BatsimEventType.JOB_COMPLETED)
        self.__job_id = str(data["job_id"])
        self.__job_state = JobState[data["job_state"]]
        self.__return_code = int(data["return_code"])
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
    def return_code(self) -> int:
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
        timestamp: the time which the event occurred.
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


class Converters:
    @staticmethod
    def profile_to_json(profile: JobProfile) -> dict:
        """ Convert a job profile to a json following batsim format.

        Args:
            profile: the profile to be converted.

        Returns:
            A data dict following batsim format.

        Raises:
            AssertionError: In case of incorrect arguments type.
            NotImplementedError: In case of not supported job profile type.
        """
        assert isinstance(profile, JobProfile)

        if isinstance(profile, DelayJobProfile):
            return {"type": "delay", "delay": profile.delay}
        elif isinstance(profile, ParallelJobProfile):
            return {
                "type": "parallel",
                "cpu": profile.cpu,
                "com": profile.com,
            }
        elif isinstance(profile, ParallelHomogeneousJobProfile):
            return {
                "type": "parallel_homogeneous",
                "cpu": profile.cpu,
                "com": profile.com,
            }
        elif isinstance(profile, ParallelHomogeneousTotalJobProfile):
            return {
                "type": "parallel_homogeneous_total",
                "cpu": profile.cpu,
                "com": profile.com,
            }
        elif isinstance(profile, ComposedJobProfile):
            return {
                "type": "composed",
                "repeat": profile.repeat,
                "seq": profile.profiles,
            }
        elif isinstance(profile, ParallelHomogeneousPFSJobProfile):
            return {
                "type": "parallel_homogeneous_pfs",
                "bytes_to_read": profile.bytes_to_read,
                "bytes_to_write": profile.bytes_to_write,
                "storage": profile.storage,
            }
        elif isinstance(profile, DataStagingJobProfile):
            return {
                "type": "data_staging",
                "nb_bytes": profile.nb_bytes,
                "from": profile.src,
                "to": profile.dest,
            }
        else:
            raise NotImplementedError('Job profile type not currently supported, '
                                      'got {}.'.format(profile))

    @staticmethod
    def json_to_profile(name: str, data: dict) -> JobProfile:
        """ Convert a json following Batsim format to a job profile instance.

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
        elif data['type'] == "composed":
            return ComposedJobProfile(name, data['seq'], data['repeat'])
        elif data['type'] == "parallel_homogeneous_pfs":
            return ParallelHomogeneousPFSJobProfile(name,
                                                    data['bytes_to_read'],
                                                    data['bytes_to_write'],
                                                    data['storage'])
        elif data['type'] == "data_staging":
            return DataStagingJobProfile(name, data['nb_bytes'], data['from'], data['to'])
        else:
            raise NotImplementedError('Job profile type not currently supported, '
                                      'got {}.'.format(data['type']))

    @staticmethod
    def json_to_power_states(properties: dict) -> Sequence[PowerState]:
        """ Get Power States from Batsim json host properties.

        Args:
            data: the properties of the job in the json sent by Batsim.

        Raises:
            RuntimeError: In case of `watt_per_state` property is not defined
                in properties.

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
    def json_to_platform(data: dict) -> Platform:
        """ Get a `Platform` from Batsim json.

        Args:
            data: the Batsim json data.

        Returns:
            A platform with all hosts.
        """
        def get_host(r, allow_sharing):
            pstates = None
            props = r.get("properties", None)
            if props and "watt_per_state" in props:
                pstates = Converters.json_to_power_states(props)

            return Host(r['id'], r['name'], pstates, allow_sharing, props)

        def get_storage(r, allow_sharing):
            props = r.get("properties", None)
            return Storage(r['id'], r['name'], allow_sharing, props)

        resources = []
        allow_sharing = data['allow_compute_sharing']
        for r in data["compute_resources"]:
            resources.append(get_host(r, allow_sharing))

        allow_sharing = data['allow_storage_sharing']
        for r in data["storage_resources"]:
            resources.append(get_storage(r, allow_sharing))

        return Platform(resources)


class JobSubmittedBatsimEvent(BatsimEvent):
    """ Batsim Job Submitted event class.

    This event is dispatched by Batsim when a job is submitted to the system.

    Args:
        timestamp: the time which the event occurred.
        data: the event data dict following the Batsim format.
    """

    def __init__(self, timestamp: float, data: dict) -> None:
        super().__init__(timestamp, BatsimEventType.JOB_SUBMITTED)
        profile = Converters.json_to_profile(
            data["job"]["profile"], data["profile"])
        self.__job = Job(
            name=data["job_id"].split(Job.WORKLOAD_SEPARATOR)[1],
            workload=data["job_id"].split(Job.WORKLOAD_SEPARATOR)[0],
            res=data["job"]["res"],
            profile=profile,
            subtime=timestamp,
            walltime=data["job"].get("walltime", None),
            user_id=data["job"].get("user_id", None),
        )
        for k, v in data['job'].items():
            if k not in ('profile', 'res', 'id', 'walltime'):
                self.__job.metadata[k] = v

    @property
    def job(self) -> Job:
        """ The job. """
        return self.__job


class JobKilledBatsimEvent(BatsimEvent):
    """ Batsim Job Killed event class.

    This event is dispatched by Batsim when a job is killed.

    Args:
        timestamp: the time which the event occurred.
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
        timestamp: the time which the event occurred.
    """

    def __init__(self, timestamp: float, data: dict = None) -> None:
        super().__init__(timestamp, BatsimEventType.REQUESTED_CALL)


class RedisConfig:
    """ Batsim Redis Config class.

    Args:
        config: the config data dict following Batsim format.
    """

    def __init__(self, config) -> None:
        self.enabled: bool = config['redis-enabled']
        self.hostname: str = config['redis-hostname']
        self.port: int = config['redis-port']
        self.prefix: str = config['redis-prefix']


class BatsimConfig:
    """ Batsim Config class.

    Args:
        data: the simulation begins event data dict following Batsim format.
    """

    def __init__(self, data) -> None:
        cfg = data['config']
        self.redis = RedisConfig(cfg)
        self.allow_compute_sharing: bool = data['allow_compute_sharing']
        self.allow_storage_sharing: bool = data['allow_storage_sharing']
        self.profiles_forwarded_on_submission: bool = cfg['profiles-forwarded-on-submission']
        self.dynamic_jobs_enabled: bool = cfg['dynamic-jobs-enabled']
        self.dynamic_jobs_acknowledged: bool = cfg['dynamic-jobs-acknowledged']
        self.profile_reuse_enabled: bool = cfg['profile-reuse-enabled']
        self.forward_unknown_events: bool = cfg['forward-unknown-events']


class SimulationBeginsBatsimEvent(BatsimEvent):
    """ Batsim Simulation Begins event class.

    This event is dispatched by Batsim when the simulation begins and includes
    all configuration parameters.

    Args:
        timestamp: the time which the event occurred.
        data: the event data dict following Batsim format.
    """

    def __init__(self, timestamp: float, data: dict) -> None:
        super().__init__(timestamp, BatsimEventType.SIMULATION_BEGINS)
        self.platform = Converters.json_to_platform(data)
        self.config = BatsimConfig(data)
        self.__workloads: Dict[str, str] = data['workloads']
        self.__profiles: Dict[str, Dict[str, JobProfile]] = defaultdict(dict)

        for w, profiles in data['profiles'].items():
            for p_name, data in profiles.items():
                profile = Converters.json_to_profile(p_name, data)
                self.__profiles[w][p_name] = profile

    @property
    def workloads(self) -> Dict[str, str]:
        """ The workloads loaded in the simulation. """
        return self.__workloads.copy()

    @property
    def profiles(self) -> Dict[str, Dict[str, JobProfile]]:
        """ The job profiles loaded from the workloads. """
        return self.__profiles.copy()


class SimulationEndsBatsimEvent(BatsimEvent):
    """ Batsim Simulation Ends event class.

    This event is dispatched by Batsim when the simulation ends.

    Args:
        timestamp: the time which the event occurred.
    """

    def __init__(self, timestamp: float, data: dict = None) -> None:
        super().__init__(timestamp, BatsimEventType.SIMULATION_ENDS)


class NotifyBatsimRequest(BatsimRequest):
    """ Batsim notify request class.

    Args:
        timestamp: the time which the notification occurred or will be sent.
        notify_type: the type of the notification.

    Raises:
        AssertionError: In case of invalid arguments type.

    Attributes:
        notify_type: The notification type.
    """

    def __init__(self, timestamp: float, notify_type: BatsimNotifyType) -> None:
        assert isinstance(notify_type, BatsimNotifyType)
        super().__init__(timestamp, BatsimRequestType.NOTIFY)
        self.notify_type = notify_type

    def _get_data_dict(self) -> dict:
        """ Export data dict following Batsim format. """
        return {"type": str(self.notify_type).lower()}


class RejectJobBatsimRequest(BatsimRequest):
    """ Batsim Reject Job request class.

    This request tells batsim to remove a job from the queue. This job
    will not be scheduled nor accounted.

    Args:
        timestamp: the time which the request should occur.
        job_id: the id of the job to be rejected.

    Raises:
        AssertionError: In case of invalid arguments type.

    Attributes:
        job_id: The job id to be rejected.
    """

    def __init__(self, timestamp: float, job_id: str) -> None:
        super().__init__(timestamp, BatsimRequestType.REJECT_JOB)
        self.job_id = job_id

    def _get_data_dict(self) -> dict:
        """ Batsim data dict.

        Returns:
            A dict with the properties following Batsim format.
        """
        return {"job_id": self.job_id}


class ExecuteJobBatsimRequest(BatsimRequest):
    """ Batsim Execute Job request class.

    This request tells batsim to execute a job on the allocated resources.

    Args:
        timestamp: the time which the request should occur.
        job_id: the id of the job to execute.
        alloc: the job allocated resources.
        storage_mapping: a dict that maps a storage to a host id.

    Raises:
        AssertionError: In case of invalid arguments types.

    Attributes:
        job_id: The id of the job to execute.
        storage_mapping: the mapping of storages to hosts.
        alloc: the job allocated resources.
    """

    def __init__(self,
                 timestamp: float,
                 job_id: str,
                 alloc: Sequence[int],
                 storage_mapping: Dict[str, int] = None) -> None:
        super().__init__(timestamp, BatsimRequestType.EXECUTE_JOB)

        if storage_mapping:
            assert isinstance(storage_mapping, dict)
            assert all(isinstance(k, str) and isinstance(v, int)
                       for k, v in storage_mapping.items())

        self.job_id = job_id
        self.storage_mapping = storage_mapping
        self.alloc = ProcSet(*alloc)

    def _get_data_dict(self) -> dict:
        """ Batsim data dict.

        Returns:
            A dict with the properties following Batsim format.
        """
        d: dict = {"job_id": self.job_id, "alloc": str(self.alloc)}
        if self.storage_mapping:
            d["storage_mapping"] = self.storage_mapping

        return d


class CallMeLaterBatsimRequest(BatsimRequest):
    """ Batsim Call Me Later request class.

    This request tells batsim to setup a callback.

    Args:
        timestamp: the time which the request must occur.
        at: the time to be called back.

    Raises:
        AssertionError: In case of invalid arguments.
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
            A dict with the properties following Batsim format.
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
        AssertionError: In case of invalid arguments.

    Attributes:
        job_ids: A sequence of ids of the jobs to be killed.
    """

    def __init__(self, timestamp: float, *job_id: str) -> None:
        super().__init__(timestamp, BatsimRequestType.KILL_JOB)
        self.job_ids = list(job_id)

    def _get_data_dict(self) -> dict:
        """ Batsim data dict.

        Returns:
            A dict with the properties following Batsim format.
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
        AssertionError: In case of invalid arguments type.

    Attributes:
        job: The registered job.
    """

    def __init__(self, timestamp: float, job: Job) -> None:
        super().__init__(timestamp, BatsimRequestType.REGISTER_JOB)
        assert isinstance(job, Job)
        self.job = job

    def _get_data_dict(self) -> dict:
        """ Batsim data dict.

        Returns:
            A dict with the properties following Batsim format.
        """
        d: dict = {
            "job_id": self.job.id,
            "job": {
                "profile": self.job.profile.name,
                "res": self.job.res,
                "id": self.job.id,
            }
        }
        if self.job.walltime:
            d['job']['walltime'] = self.job.walltime

        if self.job.user_id:
            d['job']['user_id'] = self.job.user_id

        return d


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
        AssertionError: In case of invalid arguments type.

    Attributes:
        workload_name: The profile workload name.
        profile: The registered profile.
    """

    def __init__(self, timestamp: float, workload_name: str, profile: JobProfile) -> None:
        super().__init__(timestamp, BatsimRequestType.REGISTER_PROFILE)
        assert isinstance(profile, JobProfile)

        self.workload_name = str(workload_name)
        self.profile = profile

    def _get_data_dict(self) -> dict:
        """ Batsim data dict. 

        Returns:
            A dict with the properties following Batsim format.
        """
        data = {
            "workload_name": self.workload_name,
            "profile_name": self.profile.name,
            "profile": Converters.profile_to_json(self.profile)
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
        AssertionError: In case of invalid arguments type.

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
            A dict with the properties following Batsim format.
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
        AssertionError: In case of invalid arguments.

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
        assert isinstance(job_state, JobState)
        self.job_id = str(job_id)
        self.job_state = job_state
        self.kill_reason = str(kill_reason)

    def _get_data_dict(self) -> dict:
        """ Batsim data dict. 

        Returns:
            A dict with the properties following Batsim format.
        """
        d = {
            "job_id": self.job_id,
            "job_state": str(self.job_state),
            "kill_reason": self.kill_reason,
        }
        return d


class BatsimMessageDecoder:
    """ Decodes a Batsim message.

    Args:
        msg: The raw Batsim message.

    Returns:
        A Batsim Message with all requests and events.

    Raises:
        AssertionError: In case of not supported event type.
    """

    def __init__(self) -> None:
        self.constructors = {
            BatsimEventType.JOB_SUBMITTED: JobSubmittedBatsimEvent,
            BatsimEventType.JOB_COMPLETED: JobCompletedBatsimEvent,
            BatsimEventType.JOB_KILLED: JobKilledBatsimEvent,
            BatsimEventType.RESOURCE_STATE_CHANGED: ResourcePowerStateChangedBatsimEvent,
            BatsimEventType.REQUESTED_CALL: RequestedCallBatsimEvent,
            BatsimEventType.SIMULATION_BEGINS: SimulationBeginsBatsimEvent,
            BatsimEventType.SIMULATION_ENDS: SimulationEndsBatsimEvent,
            BatsimEventType.NOTIFY: NotifyBatsimEvent,
        }

    def __call__(self, msg: dict) -> Any:
        if "type" in msg and msg["type"] in list(map(str, BatsimEventType)):
            event_type = BatsimEventType[msg["type"]]
            event_constructor = self.constructors.get(event_type, None)
            assert event_constructor
            return event_constructor(msg["timestamp"], msg["data"])
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
        """ The address string. """
        return self.__address

    @property
    def is_connected(self):
        """ Whether a socket remains opened """
        return bool(self.__socket)

    def bind(self) -> None:
        """ Create and bind a socket to the address. 

        Raises:
            SystemError: In case of a connection is already opened.
        """
        if self.is_connected:
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
            AssertionError: In case of invalid argument type.
            SystemError: In case of there is no connection opened.
        """
        assert isinstance(msg, BatsimMessage)

        if not self.is_connected:
            raise SystemError("Connection not opened.")

        self.__socket.send_json(msg.to_json())

    def recv(self) -> BatsimMessage:
        """ Receive a message. 

        The default behavior is to wait until a message arrives.

        Raises:
            SystemError: In case of there is no connection opened.
        """
        if not self.is_connected:
            raise SystemError("Connection not opened.")

        return self.__socket.recv_json(object_hook=BatsimMessageDecoder())

    def send_and_recv(self, msg: BatsimMessage) -> BatsimMessage:
        """ Send a message and wait for a response.

        Args:
            msg: The message to be sent.

        Raises:
            AssertionError: In case of invalid argument type.
            SystemError: In case of there is no connection opened.
        """
        self.send(msg)
        return self.recv()
