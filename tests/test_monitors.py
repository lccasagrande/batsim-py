from typing import Optional, Sequence

from procset import ProcSet
import pytest

import batsim_py
from batsim_py.events import SimulatorEvent
from batsim_py.events import JobEvent
from batsim_py.events import HostEvent
from batsim_py.jobs import DelayJobProfile
from batsim_py.jobs import Job
from batsim_py.jobs import JobState
from batsim_py.monitors import ConsumedEnergyMonitor, HostMonitor
from batsim_py.monitors import HostPowerStateSwitchMonitor
from batsim_py.monitors import HostStateSwitchMonitor
from batsim_py.monitors import JobMonitor
from batsim_py.monitors import SchedulerMonitor
from batsim_py.monitors import SimulationMonitor
from batsim_py.protocol import Converters
from batsim_py.resources import Host
from batsim_py.resources import Platform
from batsim_py.resources import PowerStateType

from .utils import BatsimPlatformAPI


@pytest.fixture()
def mock_simulator(mocker):
    m = mocker.patch("batsim_py.simulator.SimulatorHandler", autospec=True)
    m.is_running = False
    return m


def start_job_success(name: str,
                      alloc: Sequence[int],
                      sub_t: int,
                      start_t: int,
                      stop_t: int,
                      walltime: Optional[int] = None) -> Job:
    job = Job(name, "w", len(alloc), DelayJobProfile("a", 10), sub_t, walltime)
    job._submit(sub_t)
    job._allocate(alloc)
    job._start(start_t)
    job._terminate(stop_t, JobState.COMPLETED_SUCCESSFULLY)
    return job


def start_job_failed(name: str,
                     alloc: Sequence[int],
                     sub_t: int,
                     start_t: int,
                     stop_t: int,
                     walltime: Optional[int] = None) -> Job:
    job = Job(name, "w", len(alloc), DelayJobProfile("a", 10), sub_t, walltime)
    job._submit(sub_t)
    job._allocate(alloc)
    job._start(start_t)
    job._terminate(stop_t, JobState.COMPLETED_FAILED)
    return job


def start_job_killed(name: str,
                     alloc: Sequence[int],
                     sub_t: int,
                     start_t: int,
                     stop_t: int,
                     walltime: Optional[int] = None) -> Job:
    job = Job(name, "w", len(alloc), DelayJobProfile("a", 10), sub_t, walltime)
    job._submit(sub_t)
    job._allocate(alloc)
    job._start(start_t)
    job._terminate(stop_t, JobState.COMPLETED_KILLED)
    return job


def start_job_rejected(name: str,
                       alloc: Sequence[int],
                       sub_t: int,
                       walltime: Optional[int] = None) -> Job:
    job = Job(name, "w", len(alloc), DelayJobProfile("a", 10), sub_t, walltime)
    job._submit(sub_t)
    job._reject()
    return job


class TestJobMonitor:
    @pytest.fixture()
    def monitor(self, mock_simulator) -> JobMonitor:
        monitor = JobMonitor(mock_simulator)
        monitor.on_simulation_begins(mock_simulator)
        return monitor

    def assert_values(self, info: dict, job: Job, success: int) -> None:
        alloc = ProcSet(*job.allocation) if job.allocation else None
        assert all(len(v) == 1 for v in info.values())
        assert info['job_id'][0] == job.name
        assert info['workload_name'][0] == job.workload
        assert info['profile'][0] == job.profile.name
        assert info['submission_time'][0] == job.subtime
        assert info['requested_number_of_resources'][0] == job.res
        assert info['requested_time'][0] == job.walltime
        assert info['success'][0] == success
        assert info['final_state'][0] == str(job.state)
        assert info['starting_time'][0] == job.start_time
        assert info['execution_time'][0] == job.runtime
        assert info['finish_time'][0] == job.stop_time
        assert info['waiting_time'][0] == job.waiting_time
        assert info['turnaround_time'][0] == job.turnaround_time
        assert info['stretch'][0] == job.stretch
        assert info['allocated_resources'][0] == alloc

    def test_simulation_already_running_must_raise(self, mock_simulator):
        mock_simulator.is_running = True
        with pytest.raises(RuntimeError) as excinfo:
            JobMonitor(mock_simulator)

        assert "running" in str(excinfo.value)

    def test_subscribe(self, monitor, mock_simulator, mocker):
        calls = [
            mocker.call(SimulatorEvent.SIMULATION_BEGINS,
                        monitor.on_simulation_begins),
            mocker.call(JobEvent.COMPLETED, monitor.update_info),
            mocker.call(JobEvent.REJECTED, monitor.update_info),
        ]

        mock_simulator.subscribe.assert_has_calls(calls, True)

    def test_keys(self, monitor):
        keys = monitor.info.keys()
        assert 'job_id' in keys
        assert 'success' in keys
        assert 'allocated_resources' in keys
        assert 'starting_time' in keys
        assert 'execution_time' in keys
        assert 'finish_time' in keys
        assert 'waiting_time' in keys
        assert 'stretch' in keys
        assert 'turnaround_time' in keys
        assert 'final_state' in keys
        assert 'workload_name' in keys
        assert 'profile' in keys
        assert 'requested_number_of_resources' in keys
        assert 'requested_time' in keys
        assert 'submission_time' in keys

    def test_reset_when_sim_begins(self, monitor: JobMonitor):
        assert all(not v for v in monitor.info.values())

    def test_job_completed(self, monitor: JobMonitor):
        job = start_job_success("n", [0, 1, 10, 20], 0, 20, 100, 100)
        monitor.update_info(job)
        assert all(len(v) == 1 for v in monitor.info.values())
        self.assert_values(monitor.info, job, 1)

    def test_job_success_with_failed(self, monitor: JobMonitor):
        job = start_job_failed("n", [0, 1, 10, 20], 0, 20, 100, 100)
        monitor.update_info(job)
        assert all(len(v) == 1 for v in monitor.info.values())
        self.assert_values(monitor.info, job, 0)

    def test_job_killed(self, monitor: JobMonitor):
        job = start_job_killed("n", [0, 1, 10, 20], 0, 20, 100, 100)
        monitor.update_info(job)
        assert all(len(v) == 1 for v in monitor.info.values())
        self.assert_values(monitor.info, job, 0)

    def test_job_rejected(self, monitor: JobMonitor):
        job = start_job_rejected("n", [0, 1, 10, 20], 0)
        monitor.update_info(job)
        assert all(len(v) == 1 for v in monitor.info.values())
        self.assert_values(monitor.info, job, 0)


class TestSchedulerMonitor:
    @pytest.fixture()
    def monitor(self, mock_simulator) -> SchedulerMonitor:
        monitor = SchedulerMonitor(mock_simulator)
        monitor.on_simulation_begins(mock_simulator)
        return monitor

    def test_simulation_already_running_must_raise(self, mock_simulator):
        mock_simulator.is_running = True
        with pytest.raises(RuntimeError) as excinfo:
            SchedulerMonitor(mock_simulator)

        assert "running" in str(excinfo.value)

    def test_subscribe(self, monitor, mock_simulator, mocker):
        calls = [
            mocker.call(SimulatorEvent.SIMULATION_BEGINS,
                        monitor.on_simulation_begins),
            mocker.call(SimulatorEvent.SIMULATION_ENDS,
                        monitor.on_simulation_ends),
            mocker.call(JobEvent.COMPLETED, monitor.on_job_completed),
            mocker.call(JobEvent.SUBMITTED, monitor.on_job_submitted),
            mocker.call(JobEvent.REJECTED, monitor.on_job_rejected),
        ]

        mock_simulator.subscribe.assert_has_calls(calls, True)

    def test_keys(self, monitor):
        keys = monitor.info.keys()
        assert 'makespan' in keys
        assert 'max_slowdown' in keys
        assert 'max_stretch' in keys
        assert 'max_waiting_time' in keys
        assert 'max_turnaround_time' in keys
        assert 'mean_slowdown' in keys
        assert 'mean_pp_slowdown' in keys
        assert 'mean_stretch' in keys
        assert 'mean_waiting_time' in keys
        assert 'mean_turnaround_time' in keys
        assert 'nb_jobs' in keys
        assert 'nb_jobs_finished' in keys
        assert 'nb_jobs_killed' in keys
        assert 'nb_jobs_rejected' in keys
        assert 'nb_jobs_success' in keys

    def test_values(self, monitor: SchedulerMonitor, mock_simulator):
        job_success = start_job_success("1", [0, 1], 5, 20, 100, 50)
        monitor.on_job_submitted(job_success)
        monitor.on_job_completed(job_success)

        job_failed = start_job_failed("2", [0, 1, 2], 10, 20, 150, 50)
        monitor.on_job_submitted(job_failed)
        monitor.on_job_completed(job_failed)

        job_killed = start_job_killed("3", [0, 1, 10, 20], 10, 25, 50, 250)
        monitor.on_job_submitted(job_killed)
        monitor.on_job_completed(job_killed)

        job_rejected = start_job_rejected("4", [0], 10, 100)
        monitor.on_job_submitted(job_rejected)
        monitor.on_job_rejected(job_rejected)

        mock_simulator.current_time = 150
        monitor.on_simulation_ends(mock_simulator)

        jobs = [job_success, job_failed, job_killed, job_rejected]
        mean_slowdown = sum(j.slowdown or 0 for j in jobs) / 3
        mean_pp_slowdown = sum(j.per_processor_slowdown or 0 for j in jobs) / 3
        mean_stretch = sum(j.stretch or 0 for j in jobs) / 3
        mean_waiting_time = sum(j.waiting_time or 0 for j in jobs) / 3
        mean_turnaround_time = sum(j.turnaround_time or 0 for j in jobs) / 3
        assert monitor.info['makespan'] == 150
        assert monitor.info['max_slowdown'] == job_killed.slowdown
        assert monitor.info['max_stretch'] == job_success.stretch
        assert monitor.info['max_waiting_time'] == job_killed.waiting_time
        assert monitor.info['max_turnaround_time'] == job_failed.turnaround_time
        assert monitor.info['mean_slowdown'] == mean_slowdown
        assert monitor.info['mean_pp_slowdown'] == mean_pp_slowdown
        assert monitor.info['mean_stretch'] == mean_stretch
        assert monitor.info['mean_waiting_time'] == mean_waiting_time
        assert monitor.info['mean_turnaround_time'] == mean_turnaround_time
        assert monitor.info['nb_jobs'] == 4
        assert monitor.info['nb_jobs_finished'] == 3
        assert monitor.info['nb_jobs_killed'] == 2
        assert monitor.info['nb_jobs_rejected'] == 1
        assert monitor.info['nb_jobs_success'] == 1


class TestHostMonitor:
    @pytest.fixture()
    def monitor(self, mock_simulator):
        monitor = HostMonitor(mock_simulator)
        mock_simulator.current_time = 0
        watt_on = [(100, 200), (200, 300)]
        pstates = BatsimPlatformAPI.get_resource_properties(watt_on=watt_on)
        pstates = Converters.json_to_power_states(pstates)
        hosts = [
            Host(0, "0", pstates=pstates),
            Host(1, "1", pstates=pstates),
        ]
        mock_simulator.platform = Platform(hosts)
        monitor.on_simulation_begins(mock_simulator)
        return monitor

    def test_simulation_already_running_must_raise(self, mock_simulator):
        mock_simulator.is_running = True
        with pytest.raises(RuntimeError) as excinfo:
            HostMonitor(mock_simulator)

        assert "running" in str(excinfo.value)

    def test_subscribe(self, monitor, mock_simulator, mocker):
        calls = [
            mocker.call(SimulatorEvent.SIMULATION_BEGINS,
                        monitor.on_simulation_begins),
            mocker.call(SimulatorEvent.SIMULATION_ENDS,
                        monitor.on_simulation_ends),
            mocker.call(HostEvent.STATE_CHANGED,
                        monitor.on_host_state_changed),
            mocker.call(HostEvent.COMPUTATION_POWER_STATE_CHANGED,
                        monitor.on_host_state_changed),
        ]

        mock_simulator.subscribe.assert_has_calls(calls, True)

    def test_keys(self, monitor):
        keys = monitor.info.keys()
        assert 'time_idle' in keys
        assert 'time_computing' in keys
        assert 'time_switching_off' in keys
        assert 'time_switching_on' in keys
        assert 'time_sleeping' in keys
        assert 'consumed_joules' in keys
        assert 'energy_waste' in keys
        assert 'nb_switches' in keys
        assert 'nb_computing_machines' in keys

    def test_values(self, monitor, mock_simulator):
        host_1 = mock_simulator.platform.get_host(0)
        host_2 = mock_simulator.platform.get_host(1)

        # Switch Off
        mock_simulator.current_time = 100
        h1_e = host_1.get_default_pstate().watt_idle * 100
        h1_e_waste = h1_e

        host_1._switch_off()
        monitor.on_host_state_changed(host_1)
        assert monitor.info['consumed_joules'] == h1_e
        assert monitor.info['energy_waste'] == h1_e_waste

        # Set Off
        mock_simulator.current_time = 175
        h1_e += host_1.pstate.watt_idle * 75
        h1_e_waste = h1_e

        host_1._set_off()
        monitor.on_host_state_changed(host_1)
        assert monitor.info['consumed_joules'] == h1_e
        assert monitor.info['energy_waste'] == h1_e_waste

        # Switch On
        mock_simulator.current_time = 225
        h1_e += host_1.pstate.watt_idle * 50

        host_1._switch_on()
        monitor.on_host_state_changed(host_1)
        assert monitor.info['consumed_joules'] == h1_e
        assert monitor.info['energy_waste'] == h1_e_waste

        # Set on
        mock_simulator.current_time = 325
        h1_e += host_1.pstate.watt_idle * 100
        h1_e_waste += host_1.pstate.watt_idle * 100

        host_1._set_on()
        monitor.on_host_state_changed(host_1)
        assert monitor.info['consumed_joules'] == h1_e
        assert monitor.info['energy_waste'] == h1_e_waste

        # Computing
        mock_simulator.current_time = 425
        h1_e += host_1.pstate.watt_idle * 100
        h1_e_waste += host_1.pstate.watt_idle * 100

        host_1._allocate("j")
        host_1._start_computing()
        monitor.on_host_state_changed(host_1)
        assert monitor.info['consumed_joules'] == h1_e
        assert monitor.info['energy_waste'] == h1_e_waste

        # Idle
        mock_simulator.current_time = 525
        h1_e += host_1.pstate.watt_full * 100

        monitor.on_simulation_ends(mock_simulator)
        h2_e = host_2.pstate.watt_idle * 525

        assert monitor.info['consumed_joules'] == h1_e + h2_e
        assert monitor.info['energy_waste'] == h1_e_waste + h2_e
        assert monitor.info['time_idle'] == 725
        assert monitor.info['time_computing'] == 100
        assert monitor.info['time_switching_off'] == 75
        assert monitor.info['time_switching_on'] == 100
        assert monitor.info['time_sleeping'] == 50
        assert monitor.info['nb_switches'] == 4
        assert monitor.info['nb_computing_machines'] == 2


class TestSimulationMonitor:
    @pytest.fixture()
    def monitor(self, mock_simulator, mocker):
        monitor = SimulationMonitor(mock_simulator)
        mock_simulator.current_time = 0
        watt_on = [(100, 200), (200, 300)]
        pstates = BatsimPlatformAPI.get_resource_properties(watt_on=watt_on)
        pstates = Converters.json_to_power_states(pstates)
        hosts = [
            Host(0, "0", pstates=pstates),
            Host(1, "1", pstates=pstates),
        ]
        mock_simulator.platform = Platform(hosts)
        mocker.patch("time.time", return_value=0)
        monitor.on_simulation_begins(mock_simulator)
        return monitor

    def test_simulation_already_running_must_raise(self, mock_simulator):
        mock_simulator.is_running = True
        with pytest.raises(RuntimeError) as excinfo:
            SimulationMonitor(mock_simulator)

        assert "running" in str(excinfo.value)

    def test_subscribe(self, mock_simulator, mocker):
        mocker.patch("batsim_py.monitors.SchedulerMonitor")
        mocker.patch("batsim_py.monitors.HostMonitor")
        monitor = SimulationMonitor(mock_simulator)
        calls = [
            mocker.call(SimulatorEvent.SIMULATION_BEGINS,
                        monitor.on_simulation_begins),
            mocker.call(SimulatorEvent.SIMULATION_ENDS,
                        monitor.on_simulation_ends)
        ]

        batsim_py.monitors.HostMonitor.assert_called_once()  # type: ignore
        batsim_py.monitors.SchedulerMonitor.assert_called_once()  # type: ignore
        mock_simulator.subscribe.assert_has_calls(calls, True)

    def test_keys(self, monitor):
        keys = monitor.info.keys()
        assert 'simulation_time' in keys
        assert 'consumed_joules' in keys
        assert 'makespan' in keys

    def test_values(self, monitor, mocker):
        mocker.patch("time.time", return_value=325)
        monitor.on_simulation_ends(mock_simulator)
        assert monitor.info['simulation_time'] == 325


class TestHostStateSwitchMonitor:
    @pytest.fixture()
    def monitor(self, mock_simulator):
        monitor = HostStateSwitchMonitor(mock_simulator)
        mock_simulator.current_time = 0
        watt_on = [(100, 200), (200, 300)]
        pstates = BatsimPlatformAPI.get_resource_properties(watt_on=watt_on)
        pstates = Converters.json_to_power_states(pstates)
        hosts = [
            Host(0, "0", pstates=pstates),
            Host(1, "1", pstates=pstates),
        ]
        mock_simulator.platform = Platform(hosts)
        monitor.on_simulation_begins(mock_simulator)
        return monitor

    def test_simulation_already_running_must_raise(self, mock_simulator):
        mock_simulator.is_running = True
        with pytest.raises(RuntimeError) as excinfo:
            HostStateSwitchMonitor(mock_simulator)

        assert "running" in str(excinfo.value)

    def test_subscribe(self, monitor, mock_simulator, mocker):
        calls = [
            mocker.call(SimulatorEvent.SIMULATION_BEGINS,
                        monitor.on_simulation_begins),
            mocker.call(HostEvent.STATE_CHANGED,
                        monitor.on_host_state_changed),
        ]

        mock_simulator.subscribe.assert_has_calls(calls, True)

    def test_keys(self, monitor):
        keys = monitor.info.keys()
        assert 'time' in keys
        assert 'nb_sleeping' in keys
        assert 'nb_switching_on' in keys
        assert 'nb_idle' in keys
        assert 'nb_switching_off' in keys
        assert 'nb_computing' in keys

    def test_values(self, monitor, mock_simulator):
        host_1 = mock_simulator.platform.get_host(0)
        assert monitor.info['time'][-1] == 0
        assert monitor.info['nb_idle'][-1] == 2

        mock_simulator.current_time = 100
        host_1._switch_off()
        monitor.on_host_state_changed(host_1)
        assert monitor.info['time'][-1] == 100
        assert monitor.info['nb_idle'][-1] == 1
        assert monitor.info['nb_switching_off'][-1] == 1

        mock_simulator.current_time = 125
        host_1._set_off()
        monitor.on_host_state_changed(host_1)
        assert monitor.info['time'][-1] == 125
        assert monitor.info['nb_idle'][-1] == 1
        assert monitor.info['nb_switching_off'][-1] == 0
        assert monitor.info['nb_sleeping'][-1] == 1

        mock_simulator.current_time = 130
        host_1._switch_on()
        monitor.on_host_state_changed(host_1)
        assert monitor.info['time'][-1] == 130
        assert monitor.info['nb_idle'][-1] == 1
        assert monitor.info['nb_switching_off'][-1] == 0
        assert monitor.info['nb_sleeping'][-1] == 0
        assert monitor.info['nb_switching_on'][-1] == 1

        host_1._set_on()
        monitor.on_host_state_changed(host_1)
        assert monitor.info['time'][-1] == 130
        assert monitor.info['nb_idle'][-1] == 2
        assert monitor.info['nb_switching_off'][-1] == 0
        assert monitor.info['nb_sleeping'][-1] == 0
        assert monitor.info['nb_switching_on'][-1] == 0
        assert monitor.info['nb_computing'][-1] == 0

        host_1._allocate("job")
        host_1._start_computing()
        monitor.on_host_state_changed(host_1)
        assert monitor.info['time'][-1] == 130
        assert monitor.info['nb_idle'][-1] == 1
        assert monitor.info['nb_switching_off'][-1] == 0
        assert monitor.info['nb_sleeping'][-1] == 0
        assert monitor.info['nb_switching_on'][-1] == 0
        assert monitor.info['nb_computing'][-1] == 1


class TestHostPowerStateSwitchMonitor:
    @pytest.fixture()
    def monitor(self, mock_simulator):
        monitor = HostPowerStateSwitchMonitor(mock_simulator)
        mock_simulator.current_time = 0
        watt_on = [(100, 200), (200, 300)]
        pstates = BatsimPlatformAPI.get_resource_properties(watt_on=watt_on)
        pstates = Converters.json_to_power_states(pstates)
        hosts = [
            Host(0, "0", pstates=pstates),
            Host(1, "1", pstates=pstates),
        ]
        mock_simulator.platform = Platform(hosts)
        monitor.on_simulation_begins(mock_simulator)
        return monitor

    def test_simulation_already_running_must_raise(self, mock_simulator):
        mock_simulator.is_running = True
        with pytest.raises(RuntimeError) as excinfo:
            HostPowerStateSwitchMonitor(mock_simulator)

        assert "running" in str(excinfo.value)

    def test_subscribe(self, monitor, mock_simulator, mocker):
        calls = [
            mocker.call(SimulatorEvent.SIMULATION_BEGINS,
                        monitor.on_simulation_begins),
            mocker.call(HostEvent.COMPUTATION_POWER_STATE_CHANGED,
                        monitor.on_host_power_state_changed),
            mocker.call(HostEvent.STATE_CHANGED,
                        monitor.on_host_power_state_changed),
        ]

        mock_simulator.subscribe.assert_has_calls(calls, True)

    def test_keys(self, monitor):
        keys = monitor.info.keys()
        assert 'time' in keys
        assert 'machine_id' in keys
        assert 'new_pstate' in keys

    def test_init_values(self, monitor, mock_simulator):
        host_1 = mock_simulator.platform.get_host(0)
        assert monitor.info['time'][-1] == 0
        assert monitor.info['machine_id'][-1] == "0-1"
        assert monitor.info['new_pstate'][-1] == host_1.get_default_pstate().id

    def test_host_switch_off(self, monitor, mock_simulator):
        host_1 = mock_simulator.platform.get_host(0)
        mock_simulator.current_time = 100
        host_1._switch_off()
        monitor.on_host_power_state_changed(host_1)
        assert monitor.info['time'][-1] == 100
        assert monitor.info['machine_id'][-1] == "0"
        assert monitor.info['new_pstate'][-1] == -2

    def test_host_switch_on(self, monitor, mock_simulator):
        host_1 = mock_simulator.platform.get_host(0)
        mock_simulator.current_time = 100
        host_1._switch_off()
        monitor.on_host_power_state_changed(host_1)
        host_1._set_off()
        monitor.on_host_power_state_changed(host_1)
        host_1._switch_on()
        monitor.on_host_power_state_changed(host_1)
        assert monitor.info['time'][-1] == 100
        assert monitor.info['machine_id'][-1] == "0"
        assert monitor.info['new_pstate'][-1] == -1

    def test_host_switch_comp_ps(self, monitor, mock_simulator):
        host_1: Host = mock_simulator.platform.get_host(0)
        ps = host_1.get_pstate_by_type(PowerStateType.COMPUTATION)[-1]
        mock_simulator.current_time = 100
        host_1._set_computation_pstate(ps.id)
        monitor.on_host_power_state_changed(host_1)
        assert monitor.info['time'][-1] == 100
        assert monitor.info['machine_id'][-1] == "0"
        assert monitor.info['new_pstate'][-1] == ps.id

    def test_multiple_hosts_switch_comp_ps(self, monitor, mock_simulator):
        host_1: Host = mock_simulator.platform.get_host(0)
        host_2: Host = mock_simulator.platform.get_host(1)
        ps = host_1.get_pstate_by_type(PowerStateType.COMPUTATION)[-1]
        mock_simulator.current_time = 100
        host_1._set_computation_pstate(ps.id)
        host_2._set_computation_pstate(ps.id)
        monitor.on_host_power_state_changed(host_1)
        monitor.on_host_power_state_changed(host_2)
        assert monitor.info['time'][-1] == 100
        assert monitor.info['machine_id'][-1] == "0-1"
        assert monitor.info['new_pstate'][-1] == ps.id


class TestConsumedEnergyMonitor:
    @pytest.fixture()
    def monitor(self, mock_simulator):
        monitor = ConsumedEnergyMonitor(mock_simulator)
        mock_simulator.current_time = 0
        watt_on = [(100, 200), (200, 300)]
        pstates = BatsimPlatformAPI.get_resource_properties(watt_on=watt_on)
        pstates = Converters.json_to_power_states(pstates)
        hosts = [
            Host(0, "0", pstates=pstates),
            Host(1, "1", pstates=pstates),
        ]
        mock_simulator.platform = Platform(hosts)
        monitor.on_simulation_begins(mock_simulator)
        return monitor

    def test_simulation_already_running_must_raise(self, mock_simulator):
        mock_simulator.is_running = True
        with pytest.raises(RuntimeError) as excinfo:
            ConsumedEnergyMonitor(mock_simulator)

        assert "running" in str(excinfo.value)

    def test_subscribe(self, monitor, mock_simulator, mocker):
        calls = [
            mocker.call(SimulatorEvent.SIMULATION_BEGINS,
                        monitor.on_simulation_begins),
            mocker.call(JobEvent.STARTED,
                        monitor.on_job_started),
            mocker.call(JobEvent.COMPLETED,
                        monitor.on_job_completed),
            mocker.call(HostEvent.COMPUTATION_POWER_STATE_CHANGED,
                        monitor.on_host_state_changed),
            mocker.call(HostEvent.STATE_CHANGED,
                        monitor.on_host_state_changed),
        ]

        mock_simulator.subscribe.assert_has_calls(calls, True)

    def test_values(self, monitor: ConsumedEnergyMonitor, mock_simulator):
        host_1 = mock_simulator.platform.get_host(0)
        host_2 = mock_simulator.platform.get_host(1)
        job = start_job_success("0", [host_1.id], 0, 10, 100, 100)

        # Job Started
        mock_simulator.current_time = 10
        epower = host_1.pstate.watt_idle + host_2.pstate.watt_idle
        watt_min = host_1.pstate.watt_full + host_2.pstate.watt_full
        energy = epower * 10

        host_1._allocate(job.id)
        host_1._start_computing()
        monitor.on_job_started(job)
        assert monitor.info['time'][-1] == 10
        assert monitor.info['energy'][-1] == energy
        assert monitor.info['event_type'][-1] == "s"
        assert monitor.info['wattmin'][-1] == watt_min
        assert monitor.info['epower'][-1] == epower

        # Job Completed
        mock_simulator.current_time = 110
        epower = host_1.pstate.watt_full + host_2.pstate.watt_idle
        watt_min = host_1.pstate.watt_full + host_2.pstate.watt_full
        energy += epower * 100

        host_1._release(job.id)
        monitor.on_job_completed(job)
        assert monitor.info['time'][-1] == 110
        assert monitor.info['energy'][-1] == energy
        assert monitor.info['event_type'][-1] == "e"
        assert monitor.info['wattmin'][-1] == watt_min
        assert monitor.info['epower'][-1] == epower

        # Host Switch Off
        mock_simulator.current_time = 260
        epower = host_1.pstate.watt_idle + host_2.pstate.watt_idle
        watt_min = host_1.pstate.watt_full + host_2.pstate.watt_full
        energy += epower * 150

        host_1._switch_off()
        monitor.on_host_state_changed(host_1)
        assert monitor.info['time'][-1] == 260
        assert monitor.info['energy'][-1] == energy
        assert monitor.info['event_type'][-1] == "p"
        assert monitor.info['wattmin'][-1] == watt_min
        assert monitor.info['epower'][-1] == epower

        # Host Sleep
        mock_simulator.current_time = 300
        epower = host_1.pstate.watt_idle + host_2.pstate.watt_idle
        watt_min = host_1.pstate.watt_full + host_2.pstate.watt_full
        energy += epower * 40

        host_1._set_off()
        monitor.on_host_state_changed(host_1)
        assert monitor.info['time'][-1] == 300
        assert monitor.info['energy'][-1] == energy
        assert monitor.info['event_type'][-1] == "p"
        assert monitor.info['wattmin'][-1] == watt_min
        assert monitor.info['epower'][-1] == epower
