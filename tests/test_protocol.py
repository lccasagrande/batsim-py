from collections import defaultdict
import json

from procset import ProcSet
import pytest

from batsim_py import protocol
from batsim_py.jobs import ComposedJobProfile, Job
from batsim_py.jobs import DataStagingJobProfile
from batsim_py.jobs import DelayJobProfile
from batsim_py.jobs import JobProfile
from batsim_py.jobs import JobState
from batsim_py.jobs import ParallelHomogeneousJobProfile
from batsim_py.jobs import ParallelHomogeneousPFSJobProfile
from batsim_py.jobs import ParallelHomogeneousTotalJobProfile
from batsim_py.jobs import ParallelJobProfile
from batsim_py.protocol import BatsimMessage, BatsimMessageDecoder, NetworkHandler
from batsim_py.protocol import CallMeLaterBatsimRequest
from batsim_py.protocol import ChangeJobStateBatsimRequest
from batsim_py.protocol import ExecuteJobBatsimRequest
from batsim_py.protocol import KillJobBatsimRequest
from batsim_py.protocol import NotifyBatsimEvent
from batsim_py.protocol import BatsimNotifyType
from batsim_py.protocol import RegisterJobBatsimRequest
from batsim_py.protocol import RegisterProfileBatsimRequest
from batsim_py.protocol import RejectJobBatsimRequest
from batsim_py.protocol import SetResourceStateBatsimRequest
from batsim_py.protocol import BatsimEventType
from batsim_py.protocol import BatsimRequestType
from batsim_py.protocol import NotifyBatsimRequest
from batsim_py.protocol import JobCompletedBatsimEvent
from batsim_py.protocol import JobKilledBatsimEvent
from batsim_py.protocol import JobSubmittedBatsimEvent
from batsim_py.protocol import RequestedCallBatsimEvent
from batsim_py.protocol import ResourcePowerStateChangedBatsimEvent
from batsim_py.protocol import SimulationEndsBatsimEvent
from batsim_py.protocol import SimulationBeginsBatsimEvent
from batsim_py.protocol import Converters
from batsim_py.resources import Platform
from batsim_py.resources import PowerStateType

from .utils import BatsimAPI
from .utils import BatsimPlatformAPI
from .utils import BatsimEventAPI
from .utils import BatsimJobProfileAPI
from .utils import BatsimRequestAPI


class TestBatsimEventType:
    def test_str(self):
        e = BatsimEventType.JOB_COMPLETED
        assert str(e) == "JOB_COMPLETED"


class TestBatsimRequestType:
    def test_str(self):
        e = BatsimRequestType.CHANGE_JOB_STATE
        assert str(e) == "CHANGE_JOB_STATE"


class TestBatsimNotifyType:
    def test_str(self):
        e = BatsimNotifyType.NO_MORE_EXTERNAL_EVENT_TO_OCCUR
        assert str(e) == "NO_MORE_EXTERNAL_EVENT_TO_OCCUR"


class TestBatsimMessage:
    def test_message(self):
        events = [
            RequestedCallBatsimEvent(15),
            KillJobBatsimRequest(10, "w!1", "w!2")
        ]
        m = BatsimMessage(15, events)
        events[1], events[0] = events[0], events[1]  # sort
        assert m.now == 15
        assert m.events == events

    def test_invalid_event_timestamp_must_raise(self):
        events = [
            RequestedCallBatsimEvent(15),
            KillJobBatsimRequest(10, "w!1", "w!2")
        ]
        with pytest.raises(ValueError):
            BatsimMessage(11, events)

    def test_to_json_must_include_requests_only(self):
        events = [
            RequestedCallBatsimEvent(15),
            KillJobBatsimRequest(10, "w!1", "w!2")
        ]
        m = BatsimMessage(15, events)

        events_json = [
            BatsimRequestAPI.get_kill_job(10, ["w!1", "w!2"]),
        ]

        m_jsn = BatsimAPI.get_message(15, events_json)
        assert m.to_json() == m_jsn

    def test_events_can_be_empty(self):
        m = BatsimMessage(15)
        assert len(m.events) == 0


class TestNotifyBatsimEvent:
    def test_machine_unavailable_notify_event(self):
        jsn = BatsimEventAPI.get_notify_machine_unavailable(10, [1, 2, 3])
        e = NotifyBatsimEvent(10, jsn['data'])
        assert e.timestamp == 10
        assert e.type == BatsimEventType.NOTIFY
        assert e.notify_type == BatsimNotifyType.EVENT_MACHINE_UNAVAILABLE
        assert e.resources == [1, 2, 3]

    def test_machine_available_notify_event(self):
        jsn = BatsimEventAPI.get_notify_machine_available(10, [1, 2])
        e = NotifyBatsimEvent(10, jsn['data'])
        assert e.timestamp == 10
        assert e.type == BatsimEventType.NOTIFY
        assert e.notify_type == BatsimNotifyType.EVENT_MACHINE_AVAILABLE
        assert e.resources == [1, 2]

    def test_no_more_external_event_to_occur_notify_event(self):
        jsn = BatsimEventAPI.get_notify_no_more_external_event_to_occur(10)
        e = NotifyBatsimEvent(10, jsn['data'])
        assert e.timestamp == 10
        assert e.type == BatsimEventType.NOTIFY
        assert e.notify_type == BatsimNotifyType.NO_MORE_EXTERNAL_EVENT_TO_OCCUR

    def test_no_more_static_job_to_submit_notify_event(self):
        jsn = BatsimEventAPI.get_notify_no_more_static_job_to_submit(10)
        e = NotifyBatsimEvent(10, jsn['data'])
        assert e.timestamp == 10
        assert e.type == BatsimEventType.NOTIFY
        assert e.notify_type == BatsimNotifyType.NO_MORE_STATIC_JOB_TO_SUBMIT


class TestJobCompletedBatsimEvent:
    def test_event(self):
        event = BatsimEventAPI.get_job_completted(
            10,
            "26dceb!4",
            JobState.COMPLETED_SUCCESSFULLY,
            0,
            (0, 1, 2, 3)
        )
        e = JobCompletedBatsimEvent(10, event["data"])

        assert e.job_id == "26dceb!4"
        assert e.job_state == JobState.COMPLETED_SUCCESSFULLY
        assert e.return_code == 0
        assert e.alloc == [0, 1, 2, 3]
        assert e.type == BatsimEventType.JOB_COMPLETED
        assert e.timestamp == 10


class TestResourcePowerStateChangedBatsimEvent:
    def test_event(self):
        event = BatsimEventAPI.get_resource_state_changed(10, (1, 2, 6, 7), 42)
        e = ResourcePowerStateChangedBatsimEvent(10, event['data'])

        assert e.type == BatsimEventType.RESOURCE_STATE_CHANGED
        assert e.timestamp == 10
        assert e.resources == [1, 2, 6, 7]
        assert e.state == 42


class TestJobSubmittedBatsimEvent:
    def test_event(self):
        event = BatsimEventAPI.get_job_submitted(10,
                                                 "dyn!my_new_job",
                                                 "delay_10s",
                                                 1,
                                                 12.0,
                                                 BatsimJobProfileAPI.get_delay(10))
        e = JobSubmittedBatsimEvent(10, event["data"])

        assert e.type == BatsimEventType.JOB_SUBMITTED
        assert e.timestamp == 10
        assert e.job.id == "dyn!my_new_job"
        assert e.job.name == "my_new_job"
        assert e.job.workload == "dyn"
        assert e.job.res == 1
        assert isinstance(e.job.profile, JobProfile)
        assert e.job.subtime == 10.0
        assert e.job.walltime == 12.0
        assert e.job.user_id is None

    def test_event_with_metadata(self):
        event = BatsimEventAPI.get_job_submitted(
            10,
            "dyn!my_new_job",
            "delay_10s",
            1,
            12.0,
            BatsimJobProfileAPI.get_delay(10),
            extra_property=True)
        e = JobSubmittedBatsimEvent(10, event["data"])

        assert e.job.metadata.get('extra_property', False) == True

    def test_job_delay_submitted_event(self):
        event = BatsimEventAPI.get_job_submitted(10,
                                                 "dyn!my_new_job",
                                                 "delay_10s",
                                                 1,
                                                 12.0,
                                                 BatsimJobProfileAPI.get_delay(10))
        e = JobSubmittedBatsimEvent(10, event["data"])

        assert isinstance(e.job.profile, DelayJobProfile)
        assert e.job.profile.delay == 10
        assert e.job.profile.name == "delay_10s"

    def test_job_parallel_submitted_event(self):
        profile = BatsimJobProfileAPI.get_parallel(4)
        event = BatsimEventAPI.get_job_submitted(10,
                                                 "dyn!my_new_job",
                                                 "delay_10s",
                                                 1,
                                                 12.0,
                                                 profile)
        e = JobSubmittedBatsimEvent(10, event["data"])

        assert isinstance(e.job.profile, ParallelJobProfile)
        assert e.job.profile.cpu == profile['cpu']
        assert e.job.profile.com == profile['com']

    def test_job_parallel_homogeneous_submitted_event(self):
        profile = BatsimJobProfileAPI.get_parallel_homogeneous(50000, 0)
        event = BatsimEventAPI.get_job_submitted(10,
                                                 "dyn!my_new_job",
                                                 "delay_10s",
                                                 1,
                                                 12.0,
                                                 profile)
        e = JobSubmittedBatsimEvent(10, event["data"])

        assert isinstance(e.job.profile, ParallelHomogeneousJobProfile)
        assert e.job.profile.cpu == profile['cpu']
        assert e.job.profile.com == profile['com']

    def test_job_parallel_homogeneous_total_submitted_event(self):
        profile = BatsimJobProfileAPI.get_parallel_homogeneous_total(0, 500000)
        event = BatsimEventAPI.get_job_submitted(10,
                                                 "dyn!my_new_job",
                                                 "delay_10s",
                                                 1,
                                                 12.0,
                                                 profile)
        e = JobSubmittedBatsimEvent(10, event["data"])

        assert isinstance(e.job.profile, ParallelHomogeneousTotalJobProfile)
        assert e.job.profile.cpu == profile['cpu']
        assert e.job.profile.com == profile['com']

    def test_job_composed_submitted_event(self):
        profile = BatsimJobProfileAPI.get_composed(
            4, ["prof1", "prof2", "prof1"])
        event = BatsimEventAPI.get_job_submitted(10,
                                                 "dyn!my_new_job",
                                                 "delay_10s",
                                                 1,
                                                 12.0,
                                                 profile)
        e = JobSubmittedBatsimEvent(10, event["data"])

        assert isinstance(e.job.profile, ComposedJobProfile)
        assert e.job.profile.repeat == 4
        assert e.job.profile.profiles == ["prof1", "prof2", "prof1"]

    def test_job_parallel_homogeneous_pfs_submitted_event(self):
        profile = BatsimJobProfileAPI.get_parallel_homogeneous_pfs(
            "nfs", 100, 150)
        event = BatsimEventAPI.get_job_submitted(10,
                                                 "dyn!my_new_job",
                                                 "delay_10s",
                                                 1,
                                                 12.0,
                                                 profile)
        e = JobSubmittedBatsimEvent(10, event["data"])

        assert isinstance(e.job.profile, ParallelHomogeneousPFSJobProfile)
        assert e.job.profile.bytes_to_read == 100
        assert e.job.profile.bytes_to_write == 150
        assert e.job.profile.storage == "nfs"

    def test_job_data_staging_submitted_event(self):
        profile = BatsimJobProfileAPI.get_data_staging("nfs", "pfs", 1000)
        event = BatsimEventAPI.get_job_submitted(10,
                                                 "dyn!my_new_job",
                                                 "delay_10s",
                                                 1,
                                                 12.0,
                                                 profile)
        e = JobSubmittedBatsimEvent(10, event["data"])

        assert isinstance(e.job.profile, DataStagingJobProfile)
        assert e.job.profile.nb_bytes == 1000
        assert e.job.profile.src == "nfs"
        assert e.job.profile.dest == "pfs"


class TestJobKilledBatsimEvent:
    def test_event(self):
        event = BatsimEventAPI.get_job_killed(10, ["w0!1", "w0!2"])
        e = JobKilledBatsimEvent(10, event['data'])

        assert e.type == BatsimEventType.JOB_KILLED
        assert e.timestamp == 10
        assert e.job_ids == ["w0!1", "w0!2"]


class TestRequestedCallBatsimEvent:
    def test_event(self):
        e = RequestedCallBatsimEvent(10)
        assert e.type == BatsimEventType.REQUESTED_CALL
        assert e.timestamp == 10


class TestSimulationEndsBatsimEvent:
    def test_event(self):
        e = SimulationEndsBatsimEvent(100)
        assert e.type == BatsimEventType.SIMULATION_ENDS
        assert e.timestamp == 100


class TestSimulationBeginsBatsimEvent:

    def test_workload(self):
        workloads = {"w": "path"}
        profiles = {
            "w": {
                "delay": BatsimJobProfileAPI.get_delay(),
                "parallel": BatsimJobProfileAPI.get_parallel()
            }
        }
        event = BatsimEventAPI.get_simulation_begins(
            0, None, [], [], workloads, profiles)

        e = SimulationBeginsBatsimEvent(0, event["data"])
        assert e.type == BatsimEventType.SIMULATION_BEGINS
        assert e.timestamp == 0
        assert e.workloads == workloads

    def test_profiles(self):
        profiles = {
            "w": {
                "delay": BatsimJobProfileAPI.get_delay(),
                "parallel": BatsimJobProfileAPI.get_parallel(),
                "parallel_hom": BatsimJobProfileAPI.get_parallel_homogeneous(cpu=1, com=1),
                "parallel_hom_cpu": BatsimJobProfileAPI.get_parallel_homogeneous(cpu=1, com=0),
                "parallel_hom_com": BatsimJobProfileAPI.get_parallel_homogeneous(cpu=0, com=1),
                "parallel_hom_total": BatsimJobProfileAPI.get_parallel_homogeneous_total(),
                "composed": BatsimJobProfileAPI.get_composed(),
                "pfs": BatsimJobProfileAPI.get_parallel_homogeneous_pfs(),
                "data": BatsimJobProfileAPI.get_data_staging()
            }
        }
        event = BatsimEventAPI.get_simulation_begins(
            0, None, [], [], {"w": "path"}, profiles)

        e = SimulationBeginsBatsimEvent(0, event["data"])
        counters = defaultdict(int)
        for _, profs in e.profiles.items():
            for _, p in profs.items():
                counters[p.__class__.__name__] += 1

        assert len(counters) == 7
        assert counters.get('DelayJobProfile', 0) == 1
        assert counters.get('ParallelJobProfile', 0) == 1
        assert counters.get('ParallelHomogeneousJobProfile', 0) == 3
        assert counters.get('ParallelHomogeneousTotalJobProfile', 0) == 1
        assert counters.get('ComposedJobProfile', 0) == 1
        assert counters.get('ParallelHomogeneousPFSJobProfile', 0) == 1
        assert counters.get('DataStagingJobProfile', 0) == 1

    def test_platform_loaded(self):
        resources = [
            BatsimPlatformAPI.get_resource(0),
            BatsimPlatformAPI.get_resource(1)
        ]
        storages = [BatsimPlatformAPI.get_resource(2)]
        workloads = {"w": "path"}
        profiles = {"w": {"delay": BatsimJobProfileAPI.get_delay()}}
        event = BatsimEventAPI.get_simulation_begins(
            0, None, resources, storages, workloads, profiles)

        e = SimulationBeginsBatsimEvent(0, event["data"])
        assert len(list(e.platform.storages)) == 1
        assert len(list(e.platform.hosts)) == 2


class TestNotifyBatsimRequest:
    def test_notify_registration_finished_to_json(self):
        jsn = BatsimRequestAPI.get_notify_registration_finished(10)
        r = NotifyBatsimRequest(10, BatsimNotifyType.REGISTRATION_FINISHED)
        assert r.to_json() == jsn

    def test_notify_registration_finished(self):
        r = NotifyBatsimRequest(10, BatsimNotifyType.REGISTRATION_FINISHED)
        assert r.type == BatsimRequestType.NOTIFY
        assert r.timestamp == 10
        assert r.notify_type == BatsimNotifyType.REGISTRATION_FINISHED

    def test_notify_continue_registration_to_json(self):
        jsn = BatsimRequestAPI.get_notify_continue_registration(20)
        r = NotifyBatsimRequest(20, BatsimNotifyType.CONTINUE_REGISTRATION)
        assert r.to_json() == jsn

    def test_notify_continue_registration(self):
        r = NotifyBatsimRequest(20, BatsimNotifyType.CONTINUE_REGISTRATION)
        assert r.type == BatsimRequestType.NOTIFY
        assert r.timestamp == 20
        assert r.notify_type == BatsimNotifyType.CONTINUE_REGISTRATION


class TestRejectJobBatsimRequest:
    def test_request(self):
        r = RejectJobBatsimRequest(10, "job_id")
        assert r.timestamp == 10
        assert r.type == BatsimRequestType.REJECT_JOB
        assert r.job_id == "job_id"

    def test_to_json(self):
        jsn = BatsimRequestAPI.get_reject_job()
        r = RejectJobBatsimRequest(jsn["timestamp"], jsn["data"]["job_id"])
        assert r.to_json() == jsn


class TestExecuteJobBatsimRequest:
    def test_request(self):
        mapping = {"pfs": 1, "nfs": 3}
        r = ExecuteJobBatsimRequest(10, "w!1", [1, 2, 3], mapping)
        assert r.timestamp == 10
        assert r.type == BatsimRequestType.EXECUTE_JOB
        assert r.job_id == "w!1"
        assert str(r.alloc) == str(ProcSet(*[1, 2, 3]))
        assert r.storage_mapping == mapping

    def test_to_json(self):
        jsn = BatsimRequestAPI.get_execute_job(10, "w!1", [1, 2])
        r = ExecuteJobBatsimRequest(10, "w!1", [1, 2])
        assert r.to_json() == jsn

    def test_to_json_with_storage_mapping(self):
        mapping = {"pfs": 1, "nfs": 3}
        jsn = BatsimRequestAPI.get_execute_job(10, "w!1", [1, 2], mapping)
        r = ExecuteJobBatsimRequest(10, "w!1", [1, 2], mapping)
        assert r.to_json() == jsn


class TestCallMeLaterBatsimRequest:
    def test_request(self):
        r = CallMeLaterBatsimRequest(10.2, 15.3)

        assert r.type == BatsimRequestType.CALL_ME_LATER
        assert r.timestamp == 10.2
        assert r.at == 15.3

    def test_to_json(self):
        jsn = BatsimRequestAPI.get_call_me_later(10, 15)
        r = CallMeLaterBatsimRequest(10, 15)
        assert r.to_json() == jsn

    def test_at_less_than_timestamp_must_raise(self):
        with pytest.raises(ValueError):
            CallMeLaterBatsimRequest(10, 9.999)

    def test_at_equal_to_timestamp_must_raise(self):
        with pytest.raises(ValueError):
            CallMeLaterBatsimRequest(10, 10)


class TestKillJobBatsimRequest:
    def test_request(self):
        ids = ["w!1", "w!2"]
        r = KillJobBatsimRequest(10, *ids)

        assert r.timestamp == 10
        assert r.type == BatsimRequestType.KILL_JOB
        assert r.job_ids == ids

    def test_to_json(self):
        ids = ["w!1", "w!2"]
        jsn = BatsimRequestAPI.get_kill_job(10, ids)
        r = KillJobBatsimRequest(10, *ids)
        assert r.to_json() == jsn


class TestRegisterJobBatsimRequest:
    def test_request(self):
        j = Job("n", "w", 1, DelayJobProfile("p", 10), 10.1, 10, 1)
        r = RegisterJobBatsimRequest(10, j)
        assert r.timestamp == 10
        assert r.type == BatsimRequestType.REGISTER_JOB
        assert r.job == j

    def test_to_json(self):
        j = Job("n", "w", 1, DelayJobProfile("p", 10), 10.1, 10, 1)
        jsn = BatsimRequestAPI.get_register_job(
            10, j.name, j.workload, j.profile.name, j.res, j.walltime, user_id=j.user_id)
        r = RegisterJobBatsimRequest(10, j)

        assert r.to_json() == jsn


class TestRegisterProfileBatsimRequest:
    def test_request(self):
        p = DelayJobProfile("p", 10)
        r = RegisterProfileBatsimRequest(10, "w", p)
        assert r.timestamp == 10
        assert r.type == BatsimRequestType.REGISTER_PROFILE
        assert r.workload_name == "w"
        assert r.profile == p

    def test_to_json(self):
        prof_json = BatsimJobProfileAPI.get_delay(10)
        profile = DelayJobProfile("p", 10)
        jsn = BatsimRequestAPI.get_register_profile(10, "w", "p", prof_json)
        r = RegisterProfileBatsimRequest(10, "w", profile)

        assert r.to_json() == jsn


class TestSetResourceStateBatsimRequest:
    def test_request(self):
        r = SetResourceStateBatsimRequest(10, [1, 2, 3], 1)

        assert r.timestamp == 10
        assert r.type == BatsimRequestType.SET_RESOURCE_STATE
        assert str(r.resources) == str(ProcSet(*[1, 2, 3]))
        assert r.state == 1

    def test_empty_resources_must_raise(self):
        with pytest.raises(ValueError):
            SetResourceStateBatsimRequest(10, [], 1)

    def test_add_resource(self):
        r = SetResourceStateBatsimRequest(10, [1, 2, 3], 1)
        r.add_resource(9)
        r.add_resource(10)
        r.add_resource(15)

        assert str(r.resources) == str(ProcSet(*[1, 2, 3, 9, 10, 15]))

    def test_to_json(self):
        jsn = BatsimRequestAPI.get_set_resource_state(10, [1, 2, 3], "1")
        r = SetResourceStateBatsimRequest(10, [1, 2, 3], 1)

        assert r.to_json() == jsn


class TestChangeJobStateBatsimRequest:
    def test_request(self):
        r = ChangeJobStateBatsimRequest(
            10, "w!0", JobState.COMPLETED_KILLED, "X")

        assert r.timestamp == 10
        assert r.job_id == "w!0"
        assert r.type == BatsimRequestType.CHANGE_JOB_STATE
        assert r.job_state == JobState.COMPLETED_KILLED
        assert r.kill_reason == "X"

    def test_to_json(self):
        jsn = BatsimRequestAPI.get_change_job_state(
            10, "w!0", JobState.COMPLETED_KILLED, "X")
        r = ChangeJobStateBatsimRequest(
            10, "w!0", JobState.COMPLETED_KILLED, "X")

        assert r.to_json() == jsn


class TestBatsimMessageDecoder:
    def test_call(self):
        all_events_json = []
        for attr, obj in BatsimEventAPI.__dict__.items():
            if isinstance(obj, staticmethod):
                method = getattr(BatsimEventAPI, attr)
                all_events_json.append(method())

        msg = json.dumps(BatsimAPI.get_message(0, all_events_json))
        m = json.loads(msg, object_hook=BatsimMessageDecoder())

        calls = {t: 0 for t in BatsimEventType}
        for e in m.events:
            calls[e.type] += 1

        n = BatsimEventType.NOTIFY
        assert all(v == 1 if e != n else v == 4 for e, v in calls.items())
        assert m.now == 0
        assert len(m.events) == 11


class TestNetworkHandler:
    @pytest.fixture(autouse=True)
    def setup(self, mocker):
        mocker.patch.object(protocol.zmq.Socket, 'bind')
        mocker.patch.object(protocol.zmq.Socket, 'close')
        mocker.patch.object(protocol.zmq.Socket, 'send_json')
        mocker.patch.object(protocol.zmq.Socket, 'recv_json')

    def test_bind(self):
        a = "tcp://localhost:28000"
        p = NetworkHandler(a)
        p.bind()
        assert p.is_connected
        assert p.address == a
        protocol.zmq.Socket.bind.assert_called_once_with(a)

    def test_bind_when_connected_must_raise(self):
        p = NetworkHandler("tcp://localhost:28000")
        p.bind()
        with pytest.raises(SystemError):
            p.bind()

    def test_close(self):
        p = NetworkHandler("tcp://localhost:28000")
        p.bind()
        p.close()
        protocol.zmq.Socket.close.assert_called()

    def test_send_without_socket_must_raise(self):
        p = NetworkHandler("tcp://localhost:28000")
        with pytest.raises(SystemError):
            p.send(BatsimMessage(0))

    def test_send(self):
        msg = BatsimMessage(0)
        p = NetworkHandler("tcp://localhost:28000")
        p.bind()
        p.send(msg)
        protocol.zmq.Socket.send_json.assert_called_once_with(
            msg.to_json())

    def test_recv_without_socket_must_raise(self):
        p = NetworkHandler("tcp://localhost:28000")
        with pytest.raises(SystemError):
            p.recv()

    def test_recv(self, mocker):
        msg = BatsimMessage(0)
        mocker.patch.object(protocol.zmq.Socket, 'recv_json', return_value=msg)

        p = NetworkHandler("tcp://localhost:28000")
        p.bind()
        m = p.recv()

        assert m == msg
        protocol.zmq.Socket.recv_json.assert_called_once()

    def test_send_and_recv(self, mocker):
        msg = BatsimMessage(0)
        mocker.patch.object(protocol.zmq.Socket, 'recv_json', return_value=msg)

        p = NetworkHandler("tcp://localhost:28000")
        p.bind()

        m = p.send_and_recv(msg)

        assert m == msg
        protocol.zmq.Socket.recv_json.assert_called_once()
        protocol.zmq.Socket.send_json.assert_called_once_with(msg.to_json())


class TestConverters:
    def test_profile_to_json_delay(self):
        api = BatsimJobProfileAPI.get_delay(10)
        p = DelayJobProfile("p", 10)
        jsn = Converters.profile_to_json(p)
        assert api == jsn

    def test_profile_to_json_parallel(self):
        api = BatsimJobProfileAPI.get_parallel(10)
        p = ParallelJobProfile("p", api["cpu"], api["com"])
        jsn = Converters.profile_to_json(p)
        assert api == jsn

    def test_profile_to_json_parallel_homogeneous(self):
        api = BatsimJobProfileAPI.get_parallel_homogeneous(10, 10)
        p = ParallelHomogeneousJobProfile("p", 10, 10)
        jsn = Converters.profile_to_json(p)
        assert api == jsn

    def test_profile_to_json_parallel_homogeneous_total(self):
        api = BatsimJobProfileAPI.get_parallel_homogeneous_total(10, 10)
        p = ParallelHomogeneousTotalJobProfile("p", 10, 10)
        jsn = Converters.profile_to_json(p)
        assert api == jsn

    def test_profile_to_json_composed(self):
        api = BatsimJobProfileAPI.get_composed(1, ["1", "2"])
        p = ComposedJobProfile("p", ["1", "2"], 1)
        jsn = Converters.profile_to_json(p)
        assert api == jsn

    def test_profile_to_json_parallel_homogeneous_pfs(self):
        api = BatsimJobProfileAPI.get_parallel_homogeneous_pfs("nfs", 1, 1)
        p = ParallelHomogeneousPFSJobProfile("p", 1, 1, "nfs")
        jsn = Converters.profile_to_json(p)
        assert api == jsn

    def test_profile_to_json_data_staging(self):
        api = BatsimJobProfileAPI.get_data_staging("src", "dest", 10)
        p = DataStagingJobProfile("p", 10, "src", "dest")
        jsn = Converters.profile_to_json(p)
        assert api == jsn

    def test_profile_to_json_invalid_type_must_raise(self):
        class NewJobProfile(JobProfile):
            pass
        with pytest.raises(NotImplementedError):
            Converters.profile_to_json(NewJobProfile("p"))

    def test_json_to_profile_delay(self):
        api = BatsimJobProfileAPI.get_delay(10)
        p = Converters.json_to_profile("p", api)
        assert isinstance(p, DelayJobProfile)

    def test_json_to_profile_parallel(self):
        api = BatsimJobProfileAPI.get_parallel(10)
        p = Converters.json_to_profile("p", api)
        assert isinstance(p, ParallelJobProfile)

    def test_json_to_profile_parallel_homogeneous(self):
        api = BatsimJobProfileAPI.get_parallel_homogeneous(10, 10)
        p = Converters.json_to_profile("p", api)
        assert isinstance(p, ParallelHomogeneousJobProfile)

    def test_json_to_profile_parallel_homogeneous_total(self):
        api = BatsimJobProfileAPI.get_parallel_homogeneous_total(10, 10)
        p = Converters.json_to_profile("p", api)
        assert isinstance(p, ParallelHomogeneousTotalJobProfile)

    def test_json_to_profile_parallel_composed(self):
        api = BatsimJobProfileAPI.get_composed(1, ["1", "2"])
        p = Converters.json_to_profile("p", api)
        assert isinstance(p, ComposedJobProfile)

    def test_json_to_profile_parallel_homogeneous_pfs(self):
        api = BatsimJobProfileAPI.get_parallel_homogeneous_pfs("nfs", 1, 1)
        p = Converters.json_to_profile("p", api)
        assert isinstance(p, ParallelHomogeneousPFSJobProfile)

    def test_json_to_profile_data_staging(self):
        api = BatsimJobProfileAPI.get_data_staging("src", "dest", 10)
        p = Converters.json_to_profile("p", api)
        assert isinstance(p, DataStagingJobProfile)

    def test_json_to_profile_invalid_type_must_raise(self):
        api = BatsimJobProfileAPI.get_parallel_homogeneous_pfs("nfs", 1, 1)
        api["type"] = "NewType"
        with pytest.raises(NotImplementedError):
            Converters.json_to_profile("n", api)

    def test_json_to_power_states_without_property_must_raise(self):
        prop = BatsimPlatformAPI.get_resource_properties()
        del prop["watt_per_state"]
        with pytest.raises(RuntimeError):
            Converters.json_to_power_states(prop)

    def test_json_to_power_states(self):
        prop = BatsimPlatformAPI.get_resource_properties(
            watt_off=10,
            watt_switch_off=50,
            watt_switch_on=75,
            watt_on=[(10, 100)],
        )
        ps = Converters.json_to_power_states(prop)
        assert len(ps) == 4

        sleep_ps = next(p for p in ps if p.type == PowerStateType.SLEEP)
        s_off_ps = next(p for p in ps if p.type ==
                        PowerStateType.SWITCHING_OFF)
        s_on_ps = next(p for p in ps if p.type == PowerStateType.SWITCHING_ON)
        on_ps = next(p for p in ps if p.type == PowerStateType.COMPUTATION)

        assert sleep_ps.watt_full == 10 and sleep_ps.id == 0
        assert s_off_ps.watt_full == 50 and s_off_ps.id == 1
        assert s_on_ps.watt_full == 75 and s_on_ps.id == 2
        assert on_ps.watt_full == 100 and on_ps.watt_idle == 10 and on_ps.id == 3

    def test_json_to_platform(self):
        api = BatsimEventAPI.get_simulation_begins()["data"]
        platform = Converters.json_to_platform(api)

        assert isinstance(platform, Platform)
        assert platform.size == api["nb_resources"]
