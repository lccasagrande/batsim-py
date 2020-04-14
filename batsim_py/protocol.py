import json as json
import socket
from abc import ABC, abstractmethod
from enum import Enum
from xml.dom import minidom

import zmq
from procset import ProcSet

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
from .resources import PowerProfile
from .resources import Host
from .resources import Platform


def get_platform_from_xml(platform_fn):
    def get_speeds(data_speed):
        speeds = []
        for speed in data_speed.split(","):
            if "f" in speed:
                speeds.append(float(speed.replace("f", "")))
            elif "Mf" in speed:
                speeds.append(float(speed.replace("Mf", "")) * 1000000)
            else:
                raise NotImplementedError(
                    "Host speed must be in Mega Flops (Mf) or Flops (f)")
        return speeds

    def get_pstates(properties):
        speeds = get_speeds(properties['speed'])
        watt_per_state = properties["watt_per_state"].split(",")
        sleep_pstates = properties.get("sleep_pstates", None)

        pstates_id = {i: i for i in range(len(watt_per_state))}
        power_states = []
        if sleep_pstates:
            sleep_pstates = list(map(int, sleep_pstates.split(":")))  # values

            ps_id = pstates_id.pop(sleep_pstates[0])
            watt = float(watt_per_state[ps_id].split(":")[0])
            sleep_ps = PowerState(pstate_id=str(ps_id),
                                  pstate_type=PowerStateType.SLEEP,
                                  power_profile=PowerProfile(watt, watt),
                                  pstate_speed=speeds[ps_id])

            ps_id = pstates_id.pop(sleep_pstates[1])
            watt = float(watt_per_state[ps_id].split(":")[0])
            switchoff_ps = PowerState(pstate_id=str(ps_id),
                                      pstate_type=PowerStateType.SWITCHING_OFF,
                                      power_profile=PowerProfile(watt, watt),
                                      pstate_speed=speeds[ps_id])

            ps_id = pstates_id.pop(sleep_pstates[2])
            watt = float(watt_per_state[ps_id].split(":")[0])
            switchon_ps = PowerState(pstate_id=str(ps_id),
                                     pstate_type=PowerStateType.SWITCHING_ON,
                                     power_profile=PowerProfile(watt, watt),
                                     pstate_speed=speeds[ps_id])

            power_states.append(sleep_ps)
            power_states.append(switchoff_ps)
            power_states.append(switchon_ps)

        # Get Computation Power States
        for ps_id in pstates_id.values():
            watt = list(map(float, watt_per_state[ps_id].split(":")))
            comp_ps = PowerState(pstate_id=str(ps_id),
                                 pstate_type=PowerStateType.COMPUTATION,
                                 power_profile=PowerProfile(watt[0], watt[1]),
                                 pstate_speed=speeds[ps_id])
            power_states.append(comp_ps)

        return power_states

    platform = minidom.parse(platform_fn)
    resources = platform.getElementsByTagName('host')
    resources.sort(key=lambda x: x.attributes['id'].value)
    hosts, host_id = [], 0
    for r in resources:
        if r.getAttribute('id') == 'master_host':
            continue

        properties = {
            p.getAttribute('id'): p.getAttribute('value') for p in r.getElementsByTagName('prop')
        }
        properties['speed'] = r.getAttribute('speed')
        host_name = r.getAttribute('id')
        pstates = get_pstates(properties)

        hosts.append(Host(host_id, host_name, pstates))
        host_id += 1
    return Platform(hosts)


class BatsimMessageDecoder:
    def __call__(self, msg):
        if "type" in msg and msg["type"] in BatsimEventType.__members__:
            event_type = BatsimEventType[msg["type"]]
            if event_type == BatsimEventType.JOB_SUBMITTED:
                return JobSubmittedBatsimEvent(msg["timestamp"], msg["data"])
            elif event_type == BatsimEventType.JOB_COMPLETED:
                return JobCompletedBatsimEvent(msg["timestamp"], msg["data"])
            elif event_type == BatsimEventType.JOB_KILLED:
                return JobKilledBatsimEvent(msg["timestamp"], msg["data"])
            elif event_type == BatsimEventType.RESOURCE_STATE_CHANGED:
                return ResourcePowerStateChangedBatsimEvent(msg["timestamp"], msg["data"])
            elif event_type == BatsimEventType.REQUESTED_CALL:
                return RequestedCallBatsimEvent(msg["timestamp"])
            elif event_type == BatsimEventType.SIMULATION_BEGINS:
                allow_sharing = msg["data"].get("allow_compute_sharing", False)
                assert not allow_sharing, "Sharing resources is currently not implemented."
                return SimulationBeginsBatsimEvent(msg["timestamp"], msg["data"])
            elif event_type == BatsimEventType.SIMULATION_ENDS:
                return SimulationEndsBatsimEvent(msg["timestamp"])
            elif event_type == BatsimEventType.NOTIFY:
                return BatsimNotify(msg["timestamp"], BatsimNotifyType[msg["data"]["type"].upper()])
            else:
                return msg
        elif "now" in msg:
            return BatsimMessage(msg["now"], msg["events"])
        else:
            return msg


class BatsimEventType(Enum):
    """ Events that Batsim can dispatch """
    SIMULATION_BEGINS = 0
    SIMULATION_ENDS = 1
    JOB_SUBMITTED = 2
    JOB_COMPLETED = 3
    JOB_STARTED = 4
    JOB_KILLED = 5
    REQUESTED_CALL = 6
    NOTIFY = 7
    RESOURCE_STATE_CHANGED = 8

    def __str__(self):
        return self.name


class BatsimRequestType(Enum):
    """ Type of requests to send to Batsim """
    REJECT_JOB = 0
    EXECUTE_JOB = 1
    CALL_ME_LATER = 2
    KILL_JOB = 3
    REGISTER_JOB = 4
    REGISTER_PROFILE = 5
    SET_RESOURCE_STATE = 6
    SET_JOB_METADATA = 7
    CHANGE_JOB_STATE = 8

    def __str__(self):
        return self.name


class BatsimNotifyType(Enum):
    """ Notification types """
    NO_MORE_STATIC_JOB_TO_SUBMIT = 0
    NO_MORE_EXTERNAL_EVENT_TO_OCCUR = 1
    REGISTRATION_FINISHED = 2
    CONTINUE_REGISTRATION = 3

    def __str__(self):
        return self.name


class BatsimMessage():
    def __init__(self, now, batsim_events):
        self.now = float(now)
        # Events timestamps must be in (non-strictly) ascending order.
        self.events = sorted(batsim_events, key=lambda e: e.timestamp)

    def to_json(self):
        jsn = {
            "now": self.now,
            "events": [e.to_json() for e in self.events]
        }
        return jsn


class BatsimRequest(ABC):
    def __init__(self, timestamp, request_type):
        assert isinstance(request_type, BatsimRequestType)
        self.timestamp = float(timestamp)
        self.type = request_type

    @abstractmethod
    def _get_data_dict(self):
        raise NotImplementedError

    def _get_extra_params_dict(self):
        return {}

    def to_json(self):
        data = self._get_data_dict()
        extra_p = self._get_extra_params_dict()
        assert isinstance(data, dict), "BatsimRequest data must be a dict."
        assert isinstance(
            extra_p, dict), "BatsimRequest extra params must be a dict."

        params = {
            "timestamp": self.timestamp,
            "type": str(self.type),
            "data": data
        }

        for k, v in extra_p.items():
            params[k] = v

        return params


class BatsimNotify:
    def __init__(self, timestamp, notify_type):
        assert isinstance(notify_type, BatsimNotifyType)
        self.timestamp = float(timestamp)
        self.type = notify_type

    def to_json(self):
        jsn = {
            "timestamp": self.timestamp,
            "type": str(BatsimEventType.NOTIFY),
            "data": {"type": str(self.type).lower()}
        }
        return jsn


class BatsimEvent(ABC):
    def __init__(self, timestamp, event_type):
        assert isinstance(event_type, BatsimEventType)
        self.timestamp = timestamp
        self.type = event_type


class JobStartedBatsimEvent(BatsimEvent):
    def __init__(self, timestamp, job_id, alloc):
        super().__init__(timestamp, BatsimEventType.JOB_STARTED)
        self.job_id = job_id
        self.alloc = list(ProcSet.from_str(alloc))


class JobCompletedBatsimEvent(BatsimEvent):
    def __init__(self, timestamp, data):
        super().__init__(timestamp, BatsimEventType.JOB_COMPLETED)
        self.job_id = data["job_id"]
        self.job_state = JobState[data["job_state"]]
        self.return_code = data["return_code"]
        self.alloc = list(ProcSet.from_str(data["alloc"]))


class ResourcePowerStateChangedBatsimEvent(BatsimEvent):
    def __init__(self, timestamp, data):
        super().__init__(timestamp, BatsimEventType.RESOURCE_STATE_CHANGED)
        self.resources = list(ProcSet.from_str(data["resources"]))
        self.state = PowerStateType(int(data["state"]))


class JobSubmittedBatsimEvent(BatsimEvent):
    def __init__(self, timestamp, data):
        super().__init__(timestamp, BatsimEventType.JOB_SUBMITTED)
        self.job = Job(
            name=data["job_id"].split(Job.WORKLOAD_SEPARATOR)[1],
            workload=data["job_id"].split(Job.WORKLOAD_SEPARATOR)[0],
            res=data["job"]["res"],
            profile=self.get_profile(data["job"]["profile"], data["profile"]),
            subtime=timestamp,
            walltime=data["job"].get("walltime", None),
            user=data["job"].get("user", None),
        )

    @staticmethod
    def get_profile(name, data):
        if data['type'] == "delay":
            return DelayJobProfile(name, data['delay'])
        elif data['type'] == "parallel":
            return ParallelJobProfile(name, data['cpu'], data['com'])
        elif data['type'] == "parallel_homogeneous":
            return ParallelHomogeneousJobProfile(name, data['cpu'], data['com'])
        elif data['type'] == "parallel_homogeneous_total":
            return ParallelHomogeneousTotalJobProfile(name, data['cpu'], data['com'])
        else:
            raise NotImplementedError


class JobKilledBatsimEvent(BatsimEvent):
    def __init__(self, timestamp, data):
        super().__init__(timestamp, BatsimEventType.JOB_KILLED)
        self.job_ids = data["job_ids"]


class RequestedCallBatsimEvent(BatsimEvent):
    def __init__(self, timestamp):
        super().__init__(timestamp, BatsimEventType.REQUESTED_CALL)


class SimulationBeginsBatsimEvent(BatsimEvent):
    def __init__(self, timestamp, data):
        super().__init__(timestamp, BatsimEventType.SIMULATION_BEGINS)
        self.data = data


class SimulationEndsBatsimEvent(BatsimEvent):
    def __init__(self, timestamp):
        super().__init__(timestamp, BatsimEventType.SIMULATION_ENDS)


class RejectJobBatsimRequest(BatsimRequest):
    def __init__(self, timestamp, job_id):
        super().__init__(timestamp, BatsimRequestType.REJECT_JOB)
        self.job_id = job_id

    def _get_data_dict(self):
        return {"job_id": self.job_id}


class ExecuteJobBatsimRequest(BatsimRequest):
    def __init__(self, timestamp, job_id, alloc):
        super().__init__(timestamp, BatsimRequestType.EXECUTE_JOB)
        self.job_id = job_id
        self.alloc = ProcSet(*alloc)

    def _get_data_dict(self):
        return {"job_id": self.job_id, "alloc": str(self.alloc)}


class CallMeLaterBatsimRequest(BatsimRequest):
    def __init__(self, timestamp, at):
        assert timestamp < at, "Call me later must happen after the timestamp"
        super().__init__(timestamp, BatsimRequestType.CALL_ME_LATER)
        self.at = at

    def _get_data_dict(self):
        return {"timestamp": self.at}


class KillJobBatsimRequest(BatsimRequest):
    def __init__(self, timestamp, *job_id):
        super().__init__(timestamp, BatsimRequestType.KILL_JOB)
        self.job_ids = list(job_id)

    def _get_data_dict(self):
        return {"job_ids": self.job_ids}


class RegisterJobBatsimRequest(BatsimRequest):
    def __init__(self, timestamp, job):
        super().__init__(timestamp, BatsimRequestType.REGISTER_JOB)
        assert isinstance(job, Job)
        self.job = job

    def _get_data_dict(self):
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
    def __init__(self, timestamp, workload_name, profile):
        super().__init__(timestamp, BatsimRequestType.REGISTER_PROFILE)
        assert isinstance(profile, JobProfile)
        self.workload_name = workload_name
        self.profile = profile

    def __get_profile_params(self):
        params = {"type": str(self.profile.type).lower()}
        if isinstance(self.profile, DelayJobProfile):
            params['delay'] = self.profile.delay
        elif isinstance(self.profile, ParallelJobProfile) \
                or isinstance(self.profile, ParallelHomogeneousJobProfile) \
                or isinstance(self.profile, ParallelHomogeneousTotalJobProfile):
            params['cpu'] = self.profile.cpu
            params['com'] = self.profile.cpu
        elif isinstance(self.profile, ComposedJobProfile):
            params['repeat'] = self.profile.repeat
            params['seq'] = [p.name for p in self.profile.profiles]
        elif isinstance(self.profile, ParallelHomogeneousPFSJobProfile):
            params['bytes_to_read'] = self.profile.bytes_to_read
            params['bytes_to_write'] = self.profile.bytes_to_write
            params['storage'] = self.profile.storage
        elif isinstance(self.profile, DataStagingJobProfile):
            params['nb_bytes'] = self.profile.nb_bytes
            params['from'] = self.profile.src
            params['to'] = self.profile.dest
        else:
            raise NotImplementedError

        return params

    def _get_data_dict(self):
        data = {
            "workload_name": self.workload_name,
            "profile_name": self.profile.name,
            "profile": self.__get_profile_params()
        }
        return data


class SetResourceStateBatsimRequest(BatsimRequest):
    def __init__(self, timestamp, resources, state_id):
        assert len(resources) > 0
        super().__init__(timestamp, BatsimRequestType.SET_RESOURCE_STATE)
        self.resources = ProcSet(*resources)
        self.state = state_id

    def add_resource(self, resource_id):
        a = ProcSet(resource_id)
        self.resources.update(a)

    def _get_data_dict(self):
        return {"resources": str(self.resources), "state": str(self.state)}


class ChangeJobStateBatsimRequest(BatsimRequest):
    def __init__(self, timestamp, job_id, job_state, kill_reason):
        super().__init__(timestamp, BatsimRequestType.CHANGE_JOB_STATE)
        self.job_id = job_id
        self.job_state = job_state
        self.kill_reason = kill_reason

    def _get_data_dict(self):
        return {"job_id": self.job_id, "job_state": self.job_state, "kill_reason": self.kill_reason}


class NetworkHandler:
    def __init__(self, address, type=zmq.REP):
        self.address = address
        self.context = zmq.Context()
        self.socket = None
        self.type = type

    def send(self, msg):
        assert self.socket, "Connection not open"
        assert isinstance(msg, BatsimMessage)
        self.socket.send_json(msg.to_json())

    def flush(self, blocking=False):
        assert self.socket, "Connection not open"
        try:
            self.socket.recv()
        except zmq.Again as e:
            pass

    def recv(self):
        assert self.socket, "Connection not open"
        return self.socket.recv_json(object_hook=BatsimMessageDecoder())

    def send_and_recv(self, msg):
        self.send(msg)
        return self.recv()

    def bind(self):
        assert not self.socket, "Connection already open"
        self.socket = self.context.socket(self.type)
        self.socket.bind(self.address)

    def connect(self):
        assert not self.socket, "Connection already open"
        self.socket = self.context.socket(self.type)
        self.socket.connect(self.address)

    def close(self):
        if self.socket:
            self.socket.close()
            self.socket = None
