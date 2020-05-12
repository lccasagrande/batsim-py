import numpy as np
import pytest

import batsim_py
from batsim_py import protocol
from batsim_py import SimulatorEvent
from batsim_py import dispatcher
from batsim_py.protocol import BatsimMessage
from batsim_py.protocol import JobCompletedBatsimEvent
from batsim_py.protocol import JobSubmittedBatsimEvent
from batsim_py.protocol import NotifyBatsimEvent
from batsim_py.protocol import RequestedCallBatsimEvent
from batsim_py.protocol import ResourcePowerStateChangedBatsimEvent
from batsim_py.protocol import SimulationBeginsBatsimEvent
from batsim_py.protocol import SimulationEndsBatsimEvent
from batsim_py.resources import PowerStateType
from batsim_py.simulator import SimulatorHandler
from batsim_py.simulator import Reservation

from .utils import BatsimEventAPI
from .utils import BatsimPlatformAPI


class TestSimulatorHandler:
    @pytest.fixture(autouse=True)
    def setup(self, mocker):
        mocker.patch("batsim_py.simulator.which", return_value=True)
        mocker.patch("batsim_py.simulator.subprocess.Popen")
        mocker.patch("batsim_py.protocol.zmq.Context")
        mocker.patch.object(protocol.NetworkHandler, 'bind')
        mocker.patch.object(protocol.NetworkHandler, 'send')
        props = BatsimPlatformAPI.get_resource_properties(
            watt_on=[(90, 100), (120, 130)]
        )
        resources = [
            BatsimPlatformAPI.get_resource(0, properties=props),
            BatsimPlatformAPI.get_resource(1, properties=props),
        ]
        e = BatsimEventAPI.get_simulation_begins(resources=resources)
        events = [SimulationBeginsBatsimEvent(0, e['data'])]
        msg = BatsimMessage(0, events)
        mocker.patch.object(protocol.NetworkHandler, 'recv', return_value=msg)

    def test_batsim_not_found_must_raise(self, mocker):
        mocker.patch("batsim_py.simulator.which", return_value=None)
        with pytest.raises(ImportError) as excinfo:
            SimulatorHandler()
        assert 'Batsim' in str(excinfo.value)

    def test_start_running_must_raise(self):
        s = SimulatorHandler()
        s.start("p", "w")
        with pytest.raises(RuntimeError) as excinfo:
            s.start("p2", "w2")
        assert "running" in str(excinfo.value)

    def test_start_with_simulation_time_less_than_zero_must_raise(self):
        s = SimulatorHandler()
        with pytest.raises(ValueError) as excinfo:
            s.start("p2", "w2", simulation_time=-1)

        assert "simulation_time" in str(excinfo.value)

    def test_start_with_simulation_time_equal_to_zero_must_raise(self):
        s = SimulatorHandler()
        with pytest.raises(ValueError) as excinfo:
            s.start("p2", "w2", simulation_time=0)

        assert "simulation_time" in str(excinfo.value)

    def test_start_with_simulation_time_must_force_close(self, mocker):
        s = SimulatorHandler()
        s.start("p2", "w2", simulation_time=10)

        # setup
        e = BatsimEventAPI.get_job_submitted(res=1)
        events = [
            JobSubmittedBatsimEvent(5, e['data']),
            RequestedCallBatsimEvent(10)
        ]
        msg = BatsimMessage(10, events)
        mocker.patch.object(protocol.NetworkHandler, 'recv', return_value=msg)
        s.proceed_time()
        assert not s.is_running

    def test_start_must_dispatch_event(self):
        def foo(sender):
            self.__called = True

        self.__called = False
        dispatcher.subscribe(foo, SimulatorEvent.SIMULATION_BEGINS)
        s = SimulatorHandler()
        s.start("p", "w")
        assert self.__called

    def test_start_valid(self):
        s = SimulatorHandler()
        assert not s.is_running
        s.start("p", "w")
        assert s.is_running
        assert s.platform
        assert s.current_time == 0
        assert not s.jobs
        assert not s.is_submitter_finished
        protocol.NetworkHandler.bind.assert_called_once()

    def test_close_valid(self):
        s = SimulatorHandler()
        s.start("p", "w")
        s.close()
        assert not s.is_running

    def test_close_not_running_must_not_raise(self):
        s = SimulatorHandler()
        try:
            s.close()
        except:
            raise pytest.fail("Close raised an exception.")

    def test_close_call_dispatcher_close_connections(self, mocker):
        s = SimulatorHandler()
        mocker.patch("batsim_py.dispatcher.close_connections")
        s.start("p", "w")
        s.close()
        dispatcher.close_connections.assert_called_once()

    def test_close_call_network_close(self, mocker):
        s = SimulatorHandler()
        mocker.patch("batsim_py.protocol.NetworkHandler.close")
        s.start("p", "w")
        s.close()
        protocol.NetworkHandler.close.assert_called_once()

    def test_close_must_dispatch_event(self):
        def foo(sender): self.__called = True
        dispatcher.subscribe(foo, SimulatorEvent.SIMULATION_ENDS)
        self.__called = False
        s = SimulatorHandler()
        s.start("p", "w")
        s.close()
        assert self.__called

    def test_proceed_time_not_running_must_raise(self, mocker):
        s = SimulatorHandler()

        with pytest.raises(RuntimeError) as excinfo:
            s.proceed_time()
        assert "running" in str(excinfo.value)

    def test_proceed_time_less_than_zero_must_raise(self, mocker):
        s = SimulatorHandler()
        s.start("p", "w")

        with pytest.raises(ValueError) as excinfo:
            s.proceed_time(-1)

        assert "time" in str(excinfo.value)

    def test_proceed_time_must_go_to_next_event(self, mocker):
        s = SimulatorHandler()
        s.start("p", "w")

        e = BatsimEventAPI.get_job_submitted()
        msg = BatsimMessage(150, [JobSubmittedBatsimEvent(150, e['data'])])
        mocker.patch.object(protocol.NetworkHandler, 'recv', return_value=msg)
        mocker.patch.object(SimulatorHandler, 'set_callback')
        s.proceed_time()
        SimulatorHandler.set_callback.assert_not_called()
        assert s.current_time == 150

    def test_proceed_time_must_go_to_next_time(self, mocker):
        s = SimulatorHandler()
        s.start("p", "w")

        msg = BatsimMessage(50, [SimulationEndsBatsimEvent(50)])
        mocker.patch.object(protocol.NetworkHandler, 'recv', return_value=msg)
        mocker.patch.object(SimulatorHandler, 'set_callback')
        s.proceed_time(50)
        SimulatorHandler.set_callback.assert_called_once()

    def test_proceed_time_must_start_job(self, mocker):
        s = SimulatorHandler()
        s.start("p", "w")

        # setup
        host = s.platform.get_by_id(0)
        host._switch_off()
        host._set_off()
        e = BatsimEventAPI.get_job_submitted(res=1)
        msg = BatsimMessage(0, [JobSubmittedBatsimEvent(0, e['data'])])
        mocker.patch.object(protocol.NetworkHandler, 'recv', return_value=msg)
        s.proceed_time()
        s.allocate(e['data']['job_id'], [0])

        host._set_on()
        e = BatsimEventAPI.get_notify_no_more_static_job_to_submit(10)
        msg = BatsimMessage(10, [NotifyBatsimEvent(10, e['data'])])
        mocker.patch.object(protocol.NetworkHandler, 'recv', return_value=msg)
        s.proceed_time()
        assert s.jobs[0].is_running

    def test_callback_not_running_must_raise(self):
        def foo(p): pass
        s = SimulatorHandler()

        with pytest.raises(RuntimeError) as excinfo:
            s.set_callback(10, foo)

        assert "running" in str(excinfo.value)

    def test_callback_invalid_time_must_raise(self, mocker):
        def foo(p): pass
        s = SimulatorHandler()

        s.start("p", "w")
        msg = BatsimMessage(50, [RequestedCallBatsimEvent(50)])
        mocker.patch.object(protocol.NetworkHandler, 'recv', return_value=msg)
        s.proceed_time(50)

        with pytest.raises(ValueError) as excinfo:
            s.set_callback(50, foo)

        assert "at" in str(excinfo.value)

    def test_queue(self, mocker):
        s = SimulatorHandler()
        s.start("p", "w")

        e = [
            JobSubmittedBatsimEvent(
                0, BatsimEventAPI.get_job_submitted(job_id="w!0")['data']),
            JobSubmittedBatsimEvent(
                0, BatsimEventAPI.get_job_submitted(job_id="w!1")['data']),
        ]
        msg = BatsimMessage(150, e)
        mocker.patch.object(protocol.NetworkHandler, 'recv', return_value=msg)
        s.proceed_time()

        assert s.queue and len(s.queue) == 2
        s.allocate("w!1", [0])
        assert s.queue and len(s.queue) == 1

    def test_agenda(self, mocker):
        s = SimulatorHandler()
        s.start("p", "w")

        e = BatsimEventAPI.get_job_submitted(res=1, walltime=100)
        e = JobSubmittedBatsimEvent(0, e['data'])
        msg = BatsimMessage(0, [e])
        mocker.patch.object(protocol.NetworkHandler, 'recv', return_value=msg)
        s.proceed_time()

        s.allocate(e.job.id, [0])

        msg = BatsimMessage(10, [RequestedCallBatsimEvent(10)])
        mocker.patch.object(protocol.NetworkHandler, 'recv', return_value=msg)
        s.proceed_time()

        agenda = [
            Reservation(0, e.job.walltime - 10),
            Reservation(1, 0),
        ]

        assert s.current_time == 10
        assert list(s.agenda) == agenda

    def test_agenda_with_job_without_walltime(self, mocker):
        s = SimulatorHandler()
        s.start("p", "w")

        e = BatsimEventAPI.get_job_submitted(res=1)
        e = JobSubmittedBatsimEvent(0, e['data'])
        msg = BatsimMessage(0, [e])
        mocker.patch.object(protocol.NetworkHandler, 'recv', return_value=msg)
        s.proceed_time()

        s.allocate(e.job.id, [0])

        msg = BatsimMessage(10, [RequestedCallBatsimEvent(10)])
        mocker.patch.object(protocol.NetworkHandler, 'recv', return_value=msg)
        s.proceed_time()

        agenda = [
            Reservation(0, np.inf),
            Reservation(1, 0),
        ]

        assert s.current_time == 10
        assert list(s.agenda) == agenda

    def test_agenda_with_multiple_jobs_in_one_host(self, mocker):
        s = SimulatorHandler()
        s.start("p", "w")

        e1 = BatsimEventAPI.get_job_submitted(
            job_id="w!0", res=1, walltime=100)
        e1 = JobSubmittedBatsimEvent(0, e1['data'])
        e2 = BatsimEventAPI.get_job_submitted(
            job_id="w!1", res=1, walltime=200)
        e2 = JobSubmittedBatsimEvent(0, e2['data'])
        msg = BatsimMessage(0, [e1, e2])
        mocker.patch.object(protocol.NetworkHandler, 'recv', return_value=msg)
        s.proceed_time()

        s.allocate(e1.job.id, [0])
        s.allocate(e2.job.id, [0])

        msg = BatsimMessage(10, [RequestedCallBatsimEvent(10)])
        mocker.patch.object(protocol.NetworkHandler, 'recv', return_value=msg)
        s.proceed_time()

        agenda = [
            Reservation(0, e2.job.walltime-10),
            Reservation(1, 0),
        ]

        assert s.current_time == 10
        assert list(s.agenda) == agenda

    def test_allocate_not_running_must_raise(self):
        s = SimulatorHandler()

        with pytest.raises(RuntimeError) as excinfo:
            s.allocate("1", [1, 2])

        assert "running" in str(excinfo.value)

    def test_allocate_invalid_job_must_raise(self):
        s = SimulatorHandler()
        s.start("p", "w")

        with pytest.raises(LookupError) as excinfo:
            s.allocate("1", [0])

        assert "job" in str(excinfo.value)

    def test_allocate_invalid_host_must_raise(self, mocker):
        s = SimulatorHandler()
        s.start("p", "w")

        e = BatsimEventAPI.get_job_submitted(res=1)
        msg = BatsimMessage(150, [JobSubmittedBatsimEvent(150, e['data'])])
        mocker.patch.object(protocol.NetworkHandler, 'recv', return_value=msg)
        s.proceed_time()

        with pytest.raises(LookupError) as excinfo:
            s.allocate(e['data']['job_id'], [2])

        assert "Host" in str(excinfo.value)

    def test_allocate_must_start_job(self, mocker):
        s = SimulatorHandler()
        s.start("p", "w")

        e = BatsimEventAPI.get_job_submitted(res=1)
        msg = BatsimMessage(150, [JobSubmittedBatsimEvent(150, e['data'])])
        mocker.patch.object(protocol.NetworkHandler, 'recv', return_value=msg)
        s.proceed_time()

        assert s.queue

        s.allocate(e['data']['job_id'], [0])

        assert s.jobs[0].is_running
        assert s.platform.get_by_id(0).is_computing

    def test_allocate_must_boot_host_sleeping(self, mocker):
        s = SimulatorHandler()
        s.start("p", "w")

        # setup
        host = s.platform.get_by_id(0)
        host._switch_off()
        host._set_off()
        e = BatsimEventAPI.get_job_submitted(res=1)
        msg = BatsimMessage(150, [JobSubmittedBatsimEvent(150, e['data'])])
        mocker.patch.object(protocol.NetworkHandler, 'recv', return_value=msg)
        s.proceed_time()
        s.allocate(e['data']['job_id'], [0])

        assert s.jobs[0].is_runnable
        assert host.is_switching_on

    def test_allocate_with_switching_off_host_must_not_start_job(self, mocker):
        s = SimulatorHandler()
        s.start("p", "w")

        # setup
        host = s.platform.get_by_id(0)
        host._switch_off()
        e = BatsimEventAPI.get_job_submitted(res=1)
        msg = BatsimMessage(150, [JobSubmittedBatsimEvent(150, e['data'])])
        mocker.patch.object(protocol.NetworkHandler, 'recv', return_value=msg)
        s.proceed_time()
        s.allocate(e['data']['job_id'], [0])

        assert s.jobs[0].is_runnable
        assert host.is_switching_off

    def test_kill_job_not_running_must_raise(self):
        s = SimulatorHandler()

        with pytest.raises(RuntimeError) as excinfo:
            s.kill_job("1")

        assert "running" in str(excinfo.value)

    def test_kill_job_not_found_must_raise(self):
        s = SimulatorHandler()
        s.start("p", "w")

        with pytest.raises(LookupError) as excinfo:
            s.kill_job("1")

        assert "job" in str(excinfo.value)

    def test_kill_job(self, mocker):
        s = SimulatorHandler()
        s.start("p", "w")

        e = BatsimEventAPI.get_job_submitted(res=1)
        msg = BatsimMessage(150, [JobSubmittedBatsimEvent(150, e['data'])])
        mocker.patch.object(protocol.NetworkHandler, 'recv', return_value=msg)
        mocker.patch.object(batsim_py.jobs.Job, '_kill')

        s.proceed_time()
        s.kill_job(s.jobs[0].id)

        assert not s.jobs
        batsim_py.jobs.Job._kill.assert_called_once_with(s.current_time)

    def test_reject_job_not_running_must_raise(self, mocker):
        s = SimulatorHandler()

        with pytest.raises(RuntimeError) as excinfo:
            s.reject_job("1")

        assert "running" in str(excinfo.value)

    def test_reject_job_not_found_must_raise(self, mocker):
        s = SimulatorHandler()
        s.start("p", "w")

        with pytest.raises(LookupError) as excinfo:
            s.reject_job("1")

        assert "job" in str(excinfo.value)

    def test_reject_job(self, mocker):
        s = SimulatorHandler()
        s.start("p", "w")

        e = BatsimEventAPI.get_job_submitted(res=1)
        msg = BatsimMessage(150, [JobSubmittedBatsimEvent(150, e['data'])])
        mocker.patch.object(protocol.NetworkHandler, 'recv', return_value=msg)
        mocker.patch.object(batsim_py.jobs.Job, '_reject')
        s.proceed_time()
        s.reject_job(e['data']['job_id'])

        assert not s.jobs
        batsim_py.jobs.Job._reject.assert_called_once()

    def test_switch_on_not_running_must_raise(self):
        s = SimulatorHandler()
        with pytest.raises(RuntimeError) as excinfo:
            s.switch_on([0])
        assert 'running' in str(excinfo.value)

    def test_switch_on_not_found_must_raise(self):
        s = SimulatorHandler()
        s.start("p", "w")
        with pytest.raises(LookupError) as excinfo:
            s.switch_on([30])
        assert 'Host' in str(excinfo.value)

    def test_switch_on(self, mocker):
        mocker.patch.object(batsim_py.resources.Host, '_switch_on')
        s = SimulatorHandler()
        s.start("p", "w")
        s.switch_on([0])
        batsim_py.resources.Host._switch_on.assert_called_once()

    def test_switch_off_not_running_must_raise(self):
        s = SimulatorHandler()
        with pytest.raises(RuntimeError) as excinfo:
            s.switch_off([0])
        assert 'running' in str(excinfo.value)

    def test_switch_off_not_found_must_raise(self):
        s = SimulatorHandler()
        s.start("p", "w")
        with pytest.raises(LookupError) as excinfo:
            s.switch_off([10])
        assert 'Host' in str(excinfo.value)

    def test_switch_off(self, mocker):
        mocker.patch.object(batsim_py.resources.Host, '_switch_off')
        s = SimulatorHandler()
        s.start("p", "w")
        s.switch_off([0])
        s.switch_off([1])
        assert batsim_py.resources.Host._switch_off.call_count == 2

    def test_switch_ps_not_running_must_raise(self):
        s = SimulatorHandler()
        with pytest.raises(RuntimeError) as excinfo:
            s.switch_power_state(0, 0)
        assert 'running' in str(excinfo.value)

    def test_switch_ps_not_found_must_raise(self):
        s = SimulatorHandler()
        s.start("p", "w")
        with pytest.raises(LookupError) as excinfo:
            s.switch_power_state(10, 0)
        assert 'Host' in str(excinfo.value)

    def test_switch_ps(self, mocker):
        mocker.patch.object(batsim_py.resources.Host,
                            '_set_computation_pstate')
        s = SimulatorHandler()
        s.start("p", "w")
        s.switch_power_state(0, 1)
        batsim_py.resources.Host._set_computation_pstate.assert_called_once_with(
            1)

    def test_on_batsim_job_submitted_must_append_in_queue(self, mocker):
        s = SimulatorHandler()
        s.start("p", "w")

        # Setup Allocate
        e = BatsimEventAPI.get_job_submitted(res=1)
        job_id, job_alloc = e['data']['job_id'], [0]
        msg = BatsimMessage(150, [JobSubmittedBatsimEvent(150, e['data'])])
        mocker.patch.object(protocol.NetworkHandler, 'recv', return_value=msg)
        s.proceed_time()

        assert s.queue and s.queue[0].id == job_id

    def test_on_batsim_job_completed_must_terminate_job(self, mocker):
        s = SimulatorHandler()
        s.start("p", "w")

        # Setup Allocate
        e = BatsimEventAPI.get_job_submitted(res=1)
        job_id, job_alloc = e['data']['job_id'], [0]
        msg = BatsimMessage(150, [JobSubmittedBatsimEvent(150, e['data'])])
        mocker.patch.object(protocol.NetworkHandler, 'recv', return_value=msg)
        s.proceed_time()
        s.allocate(job_id, job_alloc)

        # Setup Completed
        mocker.patch.object(batsim_py.jobs.Job, '_terminate')
        e = BatsimEventAPI.get_job_completted(100, job_id, alloc=job_alloc)
        msg = BatsimMessage(150, [JobCompletedBatsimEvent(150, e['data'])])
        mocker.patch.object(protocol.NetworkHandler, 'recv', return_value=msg)
        s.proceed_time()

        batsim_py.jobs.Job._terminate.assert_called_once()
        assert not s.jobs

    def test_on_batsim_host_ps_changed_must_set_off(self, mocker):
        s = SimulatorHandler()
        s.start("p", "w")

        s.switch_off([0])
        assert s.platform.get_by_id(0).is_switching_off

        # Setup
        p_id = s.platform.get_by_id(0).get_sleep_pstate().id
        e = BatsimEventAPI.get_resource_state_changed(150, [0], p_id)
        e = ResourcePowerStateChangedBatsimEvent(150, e['data'])
        msg = BatsimMessage(150, [e])
        mocker.patch.object(protocol.NetworkHandler, 'recv', return_value=msg)
        s.proceed_time()

        assert s.platform.get_by_id(0).is_sleeping

    def test_on_batsim_host_ps_changed_must_set_on(self, mocker):
        s = SimulatorHandler()
        s.start("p", "w")

        s.platform.get_by_id(0)._switch_off()
        s.platform.get_by_id(0)._set_off()
        s.switch_on([0])
        assert s.platform.get_by_id(0).is_switching_on

        # Setup
        p_id = s.platform.get_by_id(0).get_default_pstate().id
        e = BatsimEventAPI.get_resource_state_changed(150, [0], p_id)
        e = ResourcePowerStateChangedBatsimEvent(150, e['data'])
        msg = BatsimMessage(150, [e])
        mocker.patch.object(protocol.NetworkHandler, 'recv', return_value=msg)
        s.proceed_time()

        assert s.platform.get_by_id(0).is_idle

    def test_on_batsim_host_ps_changed_must_set_comp_ps(self, mocker):
        s = SimulatorHandler()
        s.start("p", "w")

        # Setup
        host = s.platform.get_by_id(0)
        new_ps = host.get_pstate_by_type(PowerStateType.COMPUTATION)[-1]
        assert host.pstate != new_ps

        e = BatsimEventAPI.get_resource_state_changed(
            150, [host.id], new_ps.id)
        e = ResourcePowerStateChangedBatsimEvent(150, e['data'])
        msg = BatsimMessage(150, [e])
        mocker.patch.object(protocol.NetworkHandler, 'recv', return_value=msg)
        s.proceed_time()
        assert host.pstate == new_ps

    def test_on_batsim_simulation_ends_must_ack(self, mocker):
        s = SimulatorHandler()
        s.start("p", "w")
        msg = BatsimMessage(100, [SimulationEndsBatsimEvent(100)])
        mocker.patch.object(protocol.NetworkHandler, 'recv', return_value=msg)
        s.proceed_time()

        assert not s.is_running
        assert protocol.NetworkHandler.send.call_count == 2

    def test_is_submitter_finished_must_not_allow_callback(self, mocker):
        s = SimulatorHandler()
        s.start("p", "w")

        e = BatsimEventAPI.get_notify_no_more_static_job_to_submit(10)
        msg = BatsimMessage(10, [NotifyBatsimEvent(10, e['data'])])
        mocker.patch.object(protocol.NetworkHandler, 'recv', return_value=msg)
        s.proceed_time()
        assert s.is_submitter_finished

        msg = BatsimMessage(50, [SimulationEndsBatsimEvent(50)])
        mocker.patch.object(protocol.NetworkHandler, 'recv', return_value=msg)
        mocker.patch.object(SimulatorHandler, 'set_callback')
        s.proceed_time(100)
        assert not s.is_running
        assert s.current_time == 50
        SimulatorHandler.set_callback.assert_not_called()

    def test_is_submitter_finished_with_queue_must_allow_callback(self, mocker):
        s = SimulatorHandler()
        s.start("p", "w")

        e = BatsimEventAPI.get_notify_no_more_static_job_to_submit(10)
        e2 = BatsimEventAPI.get_job_submitted()
        events = [
            JobSubmittedBatsimEvent(10, e2['data']),
            NotifyBatsimEvent(10, e['data']),
        ]
        msg = BatsimMessage(10, events)
        mocker.patch.object(protocol.NetworkHandler, 'recv', return_value=msg)
        s.proceed_time()
        assert s.is_submitter_finished

    def test_is_submitter_finished_with_sim_time_must_allow_callback(self, mocker):
        s = SimulatorHandler()
        s.start("p", "w", simulation_time=100)

        e = BatsimEventAPI.get_notify_no_more_static_job_to_submit(10)
        msg = BatsimMessage(10, [NotifyBatsimEvent(10, e['data'])])
        mocker.patch.object(protocol.NetworkHandler, 'recv', return_value=msg)
        s.proceed_time()
        assert s.is_submitter_finished

        msg = BatsimMessage(50, [SimulationEndsBatsimEvent(50)])
        mocker.patch.object(protocol.NetworkHandler, 'recv', return_value=msg)
        mocker.patch.object(SimulatorHandler, 'set_callback')
        s.proceed_time(50)
        SimulatorHandler.set_callback.assert_called_once()

    def test_on_batsim_no_more_jobs_to_submit_with_simulation_time(self, mocker):
        s = SimulatorHandler()
        s.start("p", "w", simulation_time=1000)

        e = BatsimEventAPI.get_notify_no_more_static_job_to_submit(10)
        msg = BatsimMessage(10, [NotifyBatsimEvent(10, e['data'])])
        mocker.patch.object(protocol.NetworkHandler, 'recv', return_value=msg)
        s.proceed_time()

        assert s.is_running
        assert s.is_submitter_finished

        msg = BatsimMessage(1000, [RequestedCallBatsimEvent(1000)])
        mocker.patch.object(protocol.NetworkHandler, 'recv', return_value=msg)
        s.proceed_time(900)
        assert not s.is_running
        assert s.current_time == 1000

    def test_on_batsim_host_ps_changed_must_start_jobs_in_queue(self, mocker):
        s = SimulatorHandler()
        s.start("p", "w")

        host = s.platform.get_by_id(0)
        host._switch_off()
        host._set_off()
        host._switch_on()

        # Setup Allocate
        e = BatsimEventAPI.get_job_submitted(res=1)['data']
        e = JobSubmittedBatsimEvent(0, e)
        msg = BatsimMessage(0, [e])
        mocker.patch.object(protocol.NetworkHandler, 'recv', return_value=msg)
        s.proceed_time()

        s.allocate(e.job.id, [0])

        # Setup Host changed state
        p_id = host.get_default_pstate().id
        e = BatsimEventAPI.get_resource_state_changed(150, [0], p_id)
        e = ResourcePowerStateChangedBatsimEvent(150, e['data'])
        msg = BatsimMessage(150, [e])
        mocker.patch.object(protocol.NetworkHandler, 'recv', return_value=msg)
        s.proceed_time()

        assert not s.queue
        assert s.jobs[0].is_running
