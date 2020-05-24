
from abc import ABC, abstractmethod
from itertools import groupby
import time as tm
from typing import Optional

import pandas as pd
from procset import ProcSet

from .events import SimulatorEvent
from .events import JobEvent
from .events import HostEvent
from .jobs import JobState
from .jobs import Job
from .resources import HostState
from .resources import Host
from .resources import PowerStateType
from .simulator import SimulatorHandler


class Monitor(ABC):
    """ Simulation Monitor base class.

    Args:
        simulator: The simulator handler.

    Raises:
        RuntimeError: In case of the simulation is already running.
    """

    def __init__(self, simulator: SimulatorHandler):
        if simulator.is_running:
            raise RuntimeError('Monitors cannot be created when the simulation '
                               'is already running.')
        self.simulator = simulator

    @property
    @abstractmethod
    def info(self) -> dict:
        """ Get monitor info about the simulation.

        Returns:
            A dict with all the information collected.
        """
        raise NotImplementedError

    @abstractmethod
    def to_csv(self, fn: str) -> None:
        """ Dump info a csv file. """
        raise NotImplementedError

    @abstractmethod
    def to_dataframe(self) -> pd.DataFrame:
        """ Return a Dataframe object. """
        raise NotImplementedError


class JobMonitor(Monitor):
    """ Simulation Job Monitor class.

    This monitor collects statistics from each job submitted to the system.

    Args:
        simulator: The simulator handler.

    Raises:
        RuntimeError: In case of the simulation is already running.
    """

    def __init__(self, simulator: SimulatorHandler) -> None:
        super().__init__(simulator)
        self.__info: dict = {
            'job_id': [],
            'workload_name': [],
            'profile': [],
            'submission_time': [],
            'requested_number_of_resources': [],
            'requested_time': [],
            'success': [],
            'final_state': [],
            'starting_time': [],
            'execution_time': [],
            'finish_time': [],
            'waiting_time': [],
            'turnaround_time': [],
            'stretch': [],
            'allocated_resources': [],
            'consumed_energy': [],
        }

        simulator.subscribe(SimulatorEvent.SIMULATION_BEGINS,
                            self.on_simulation_begins)
        simulator.subscribe(JobEvent.COMPLETED, self.update_info)
        simulator.subscribe(JobEvent.REJECTED, self.update_info)

    @property
    def info(self) -> dict:
        """ Get monitor info about the simulation.

        Returns:
            A dict with all the information collected.
        """
        return self.__info

    def to_csv(self, fn: str) -> None:
        """ Dump info a csv file. """
        self.to_dataframe().to_csv(fn, index=False)

    def to_dataframe(self) -> pd.DataFrame:
        """ Return a Dataframe object. """
        return pd.DataFrame.from_dict(self.__info)

    def on_simulation_begins(self, sender) -> None:
        self.__info = {k: [] for k in self.__info.keys()}

    def update_info(self, sender: Job) -> None:
        """ Record job statistics. """
        alloc = ProcSet(*sender.allocation) if sender.allocation else None
        success = int(sender.state == JobState.COMPLETED_SUCCESSFULLY)
        self.__info['job_id'].append(sender.name)
        self.__info['workload_name'].append(sender.workload)
        self.__info['profile'].append(sender.profile.name)
        self.__info['submission_time'].append(sender.subtime)
        self.__info['requested_number_of_resources'].append(sender.res)
        self.__info['requested_time'].append(sender.walltime)
        self.__info['success'].append(success)
        self.__info['final_state'].append(str(sender.state))
        self.__info['starting_time'].append(sender.start_time)
        self.__info['execution_time'].append(sender.runtime)
        self.__info['finish_time'].append(sender.stop_time)
        self.__info['waiting_time'].append(sender.waiting_time)
        self.__info['turnaround_time'].append(sender.turnaround_time)
        self.__info['stretch'].append(sender.stretch)
        self.__info['allocated_resources'].append(alloc)
        self.__info['consumed_energy'].append(-1)


class SchedulerMonitor(Monitor):
    """ Simulation Scheduler Monitor class.

    This monitor collects statistics about the scheduler performance.

    Args:
        simulator: The simulator handler.

    Raises:
        RuntimeError: In case of the simulation is already running.
    """

    def __init__(self, simulator: SimulatorHandler) -> None:
        super().__init__(simulator)
        self.__info: dict = {
            'makespan': 0,
            'max_slowdown': 0,
            'max_stretch': 0,
            'max_waiting_time': 0,
            'max_turnaround_time': 0,
            'mean_slowdown': 0,
            'mean_pp_slowdown': 0,
            'mean_stretch': 0,
            'mean_waiting_time': 0,
            'mean_turnaround_time': 0,
            'nb_jobs': 0,
            'nb_jobs_finished': 0,
            'nb_jobs_killed': 0,
            'nb_jobs_rejected': 0,
            'nb_jobs_success': 0,
        }

        self.simulator.subscribe(
            SimulatorEvent.SIMULATION_BEGINS, self.on_simulation_begins)
        self.simulator.subscribe(
            SimulatorEvent.SIMULATION_ENDS, self.on_simulation_ends)
        self.simulator.subscribe(JobEvent.COMPLETED, self.on_job_completed)
        self.simulator.subscribe(JobEvent.SUBMITTED, self.on_job_submitted)
        self.simulator.subscribe(JobEvent.REJECTED, self.on_job_rejected,)

    @property
    def info(self) -> dict:
        """ Get monitor info about the simulation.

        Returns:
            A dict with all the information collected.
        """
        return self.__info

    def to_csv(self, fn: str) -> None:
        """ Dump info a csv file. """
        self.to_dataframe().to_csv(fn, index=False)

    def to_dataframe(self) -> pd.DataFrame:
        """ Return a Dataframe object. """
        return pd.DataFrame.from_dict(self.__info, orient='index').T

    def on_simulation_begins(self, sender: SimulatorHandler) -> None:
        self.__info = {k: 0 for k in self.__info.keys()}
        self.simulator = sender

    def on_simulation_ends(self, sender: SimulatorHandler) -> None:
        """ Compute the average. """
        self.__info['makespan'] = sender.current_time
        nb_finished = max(1, self.__info['nb_jobs_finished'])
        self.__info['mean_waiting_time'] /= nb_finished
        self.__info['mean_slowdown'] /= nb_finished
        self.__info['mean_pp_slowdown'] /= nb_finished
        self.__info['mean_stretch'] /= nb_finished
        self.__info['mean_turnaround_time'] /= nb_finished

    def on_job_submitted(self, sender: Job) -> None:
        self.__info['nb_jobs'] += 1

    def on_job_rejected(self, sender: Job) -> None:
        self.__info['nb_jobs_rejected'] += 1

    def on_job_completed(self, sender: Job) -> None:
        """ Get job statistics. """
        assert self.simulator

        self.__info['makespan'] = self.simulator.current_time
        self.__info['mean_slowdown'] += sender.slowdown
        self.__info['mean_pp_slowdown'] += sender.per_processor_slowdown
        self.__info['mean_stretch'] += sender.stretch
        self.__info['mean_waiting_time'] += sender.waiting_time
        self.__info['mean_turnaround_time'] += sender.turnaround_time

        self.__info['max_waiting_time'] = max(
            sender.waiting_time, self.__info['max_waiting_time'])
        self.__info['max_stretch'] = max(
            sender.stretch, self.__info['max_stretch'])
        self.__info['max_slowdown'] = max(
            sender.slowdown, self.__info['max_slowdown'])
        self.__info['max_turnaround_time'] = max(
            sender.turnaround_time, self.__info['max_turnaround_time'])

        self.__info['nb_jobs_finished'] += 1
        if sender.state == JobState.COMPLETED_SUCCESSFULLY:
            self.__info['nb_jobs_success'] += 1
        elif sender.state == JobState.COMPLETED_KILLED or sender.state == JobState.COMPLETED_WALLTIME_REACHED or sender.state == JobState.COMPLETED_FAILED:
            self.__info['nb_jobs_killed'] += 1


class HostMonitor(Monitor):
    """ Simulation Host Monitor class.

    This monitor collects statistics about the time each host spent on each
    of its possible states. Moreover, it computes the total energy consumed
    and the total number of state switches.

    Args:
        simulator: The simulator handler.

    Raises:
        RuntimeError: In case of the simulation is already running.
    """

    def __init__(self, simulator: SimulatorHandler) -> None:
        super().__init__(simulator)
        self.__last_state: dict = {}
        self.__info: dict = {
            'time_idle':  0,
            'time_computing':  0,
            'time_switching_off':  0,
            'time_switching_on':  0,
            'time_sleeping':  0,
            'consumed_joules':  0,
            'energy_waste':  0,
            'nb_switches': 0,
            'nb_computing_machines': 0,
        }

        self.simulator.subscribe(
            SimulatorEvent.SIMULATION_BEGINS, self.on_simulation_begins)
        self.simulator.subscribe(
            SimulatorEvent.SIMULATION_ENDS, self.on_simulation_ends)
        self.simulator.subscribe(
            HostEvent.STATE_CHANGED, self.on_host_state_changed)
        self.simulator.subscribe(
            HostEvent.COMPUTATION_POWER_STATE_CHANGED, self.on_host_state_changed)

    @property
    def info(self) -> dict:
        """ Get monitor info about the simulation.

        Returns:
            A dict with all the information collected.
        """
        return self.__info

    def to_csv(self, fn: str) -> None:
        """ Dump info a csv file. """
        self.to_dataframe().to_csv(fn, index=False)

    def to_dataframe(self) -> pd.DataFrame:
        """ Return a Dataframe object. """
        return pd.DataFrame.from_dict(self.__info, orient='index').T

    def on_simulation_begins(self, sender: SimulatorHandler) -> None:
        self.simulator = sender
        assert self.simulator.platform
        t_now = self.simulator.current_time

        self.__last_state = {}
        for h in self.simulator.platform.hosts:
            self.__last_state[h.id] = (t_now, h.power, h.state, h.pstate)

        self.__info = {k: 0 for k in self.__info.keys()}
        self.__info['nb_computing_machines'] = self.simulator.platform.size

    def on_simulation_ends(self, sender: SimulatorHandler) -> None:
        assert self.simulator and self.simulator.platform
        for h in self.simulator.platform.hosts:
            self.update_info(h)

    def on_host_state_changed(self, sender: Host) -> None:
        self.update_info(sender)

    def update_info(self, h: Host) -> None:
        assert self.simulator
        t_start, power, state, pstate = self.__last_state[h.id]

        # Update Info
        time_spent = self.simulator.current_time - t_start
        energy_consumption = time_spent * (power or 0)
        energy_wasted = 0

        if state == HostState.IDLE:
            self.__info['time_idle'] += time_spent
            energy_wasted = energy_consumption
        elif state == HostState.COMPUTING:
            self.__info['time_computing'] += time_spent
        elif state == HostState.SWITCHING_OFF:
            self.__info['time_switching_off'] += time_spent
            energy_wasted = energy_consumption
        elif state == HostState.SWITCHING_ON:
            self.__info['time_switching_on'] += time_spent
            energy_wasted = energy_consumption
        elif state == HostState.SLEEPING:
            self.__info['time_sleeping'] += time_spent

        self.__info['consumed_joules'] += energy_consumption
        self.__info['energy_waste'] += energy_wasted

        if h.pstate and pstate.id != h.pstate.id:
            self.__info['nb_switches'] += 1

        # Update Last State
        t_now = self.simulator.current_time
        self.__last_state[h.id] = (t_now, h.power, h.state, h.pstate)


class SimulationMonitor(Monitor):
    """ Simulation Monitor class.

    This monitor collects statistics about the simulation. It merges the
    statistics from the Scheduler Monitor with the Host Monitor.

    Args:
        simulator: The simulator handler.

    Raises:
        RuntimeError: In case of the simulation is already running.
    """

    def __init__(self, simulator: SimulatorHandler) -> None:
        super().__init__(simulator)
        self.__sched_monitor = SchedulerMonitor(simulator)
        self.__hosts_monitor = HostMonitor(simulator)
        self.__sim_start_time = self.__simulation_time = -1.

        self.simulator.subscribe(
            SimulatorEvent.SIMULATION_BEGINS, self.on_simulation_begins)

        self.simulator.subscribe(
            SimulatorEvent.SIMULATION_ENDS, self.on_simulation_ends)

    @property
    def info(self) -> dict:
        """ Get monitor info about the simulation.

        Returns:
            A dict with all the information collected.
        """
        info = dict(self.__sched_monitor.info, **self.__hosts_monitor.info)
        info['simulation_time'] = self.__simulation_time
        return info

    def to_csv(self, fn: str) -> None:
        """ Dump info a csv file. """
        self.to_dataframe().to_csv(fn, index=False)

    def to_dataframe(self) -> pd.DataFrame:
        """ Return a Dataframe object. """
        return pd.DataFrame.from_dict(self.info, orient='index').T

    def on_simulation_begins(self, sender: SimulatorHandler) -> None:
        self.__sim_start_time = tm.time()

    def on_simulation_ends(self, sender: SimulatorHandler) -> None:
        self.__simulation_time = tm.time() - self.__sim_start_time


class HostStateSwitchMonitor(Monitor):
    """ Simulation Host State Monitor class.

    This monitor collects statistics about the host state switches. It'll
    record the number of hosts on each state when any host switches its state.

    Args:
        simulator: The simulator handler.

    Raises:
        RuntimeError: In case of the simulation is already running.
    """

    def __init__(self, simulator: SimulatorHandler) -> None:
        super().__init__(simulator)

        self.__last_host_state: dict = {}
        self.__info: dict = {
            'time': [],
            'nb_sleeping': [],
            'nb_switching_on': [],
            'nb_switching_off': [],
            'nb_idle': [],
            'nb_computing': []
        }
        self.simulator.subscribe(
            SimulatorEvent.SIMULATION_BEGINS, self.on_simulation_begins)

        self.simulator.subscribe(
            HostEvent.STATE_CHANGED, self.on_host_state_changed)

    @property
    def info(self) -> dict:
        """ Get monitor info about the simulation.

        Returns:
            A dict with all the information collected.
        """
        return self.__info

    def to_csv(self, fn: str) -> None:
        """ Dump info a csv file. """
        self.to_dataframe().to_csv(fn, index=False)

    def to_dataframe(self) -> pd.DataFrame:
        """ Return a Dataframe object. """
        return pd.DataFrame.from_dict(self.__info)

    def on_simulation_begins(self, sender: SimulatorHandler) -> None:
        self.simulator = sender
        assert self.simulator.platform
        self.__last_host_state = {}
        self.__info = {k: [0] for k in self.__info.keys()}

        # Update initial state
        self.__info['time'][-1] = self.simulator.current_time
        for h in self.simulator.platform.hosts:
            key = self.__get_key_from_state(h.state)
            self.__last_host_state[h.id] = key
            self.__info[key][-1] += 1

    def on_host_state_changed(self, sender: Host) -> None:
        assert self.simulator
        if self.__info['time'][-1] != self.simulator.current_time:
            # Update new info from last one
            for k in self.__info.keys():
                self.__info[k].append(self.__info[k][-1])

            self.__info['time'][-1] = self.simulator.current_time

        # Update info
        old_key = self.__last_host_state[sender.id]
        new_key = self.__get_key_from_state(sender.state)
        self.__info[old_key][-1] -= 1
        self.__info[new_key][-1] += 1
        self.__last_host_state[sender.id] = new_key

    def __get_key_from_state(self, state: HostState) -> str:
        """ Convert a host state to a dict key. """
        if state == HostState.IDLE:
            return "nb_idle"
        elif state == HostState.COMPUTING:
            return "nb_computing"
        elif state == HostState.SLEEPING:
            return "nb_sleeping"
        elif state == HostState.SWITCHING_OFF:
            return "nb_switching_off"
        elif state == HostState.SWITCHING_ON:
            return "nb_switching_on"
        else:
            raise NotImplementedError


class HostPowerStateSwitchMonitor(Monitor):
    """ Simulation Host Power State Monitor class.

    This monitor collects statistics about the host power state switches. It'll
    record an entry when a host switches its power state.

    Args:
        simulator: The simulator handler.

    Raises:
        RuntimeError: In case of the simulation is already running.
    """

    def __init__(self, simulator: SimulatorHandler) -> None:
        super().__init__(simulator)

        self.__last_pstate_id: dict = {}
        self.__info: dict = {'time': [], 'machine_id': [], 'new_pstate': []}

        self.simulator.subscribe(
            SimulatorEvent.SIMULATION_BEGINS, self.on_simulation_begins)

        self.simulator.subscribe(
            HostEvent.COMPUTATION_POWER_STATE_CHANGED, self.on_host_power_state_changed)
        self.simulator.subscribe(
            HostEvent.STATE_CHANGED, self.on_host_power_state_changed)

    @property
    def info(self) -> dict:
        """ Get monitor info about the simulation.

        Returns:
            A dict with all the information collected.
        """
        return self.__info

    def to_csv(self, fn: str) -> None:
        """ Dump info a csv file. """
        self.to_dataframe().to_csv(fn, index=False)

    def to_dataframe(self) -> pd.DataFrame:
        """ Return a Dataframe object. """
        return pd.DataFrame.from_dict(self.__info)

    def on_simulation_begins(self, sender: SimulatorHandler) -> None:
        self.simulator = sender
        assert self.simulator.platform

        self.__info = {k: [] for k in self.__info.keys()}

        hosts = [h for h in self.simulator.platform.hosts if h.pstate]

        self.__last_pstate_id = {
            h.id: h.pstate.id for h in hosts}  # type:ignore

        # Record initial state
        for p, hs in groupby(hosts, key=lambda h: h.pstate.id):  # type:ignore
            self.__info['time'].append(self.simulator.current_time)
            procset = ProcSet(*[h.id for h in hs])
            self.__info['machine_id'].append(str(procset))
            self.__info['new_pstate'].append(p)

    def on_host_power_state_changed(self, sender: Host) -> None:
        if sender.id in self.__last_pstate_id and sender.pstate and self.__last_pstate_id[sender.id] != sender.pstate.id:
            assert sender.pstate
            assert self.simulator

            if self.__last_pstate_id[sender.id] == sender.pstate.id:
                return

            self.__last_pstate_id[sender.id] = sender.pstate.id
            if sender.pstate.type == PowerStateType.SWITCHING_OFF:
                new_pstate_id = -2
            elif sender.pstate.type == PowerStateType.SWITCHING_ON:
                new_pstate_id = -1
            else:
                new_pstate_id = sender.pstate.id

            if self.__info['time'][-1] == self.simulator.current_time and self.__info['new_pstate'][-1] == new_pstate_id:
                # Update last record
                procset = ProcSet.from_str(self.__info['machine_id'][-1])
                procset.update(ProcSet(sender.id))
                self.__info['machine_id'][-1] = str(procset)
            else:
                # Record new state
                self.__info['time'].append(self.simulator.current_time)
                self.__info['machine_id'].append(str(sender.id))
                self.__info['new_pstate'].append(new_pstate_id)


class ConsumedEnergyMonitor(Monitor):
    """ Simulation Energy Monitor class.

    This monitor collects energy statistics. It'll record the platform consumed
    energy when a job starts/finish or a host changes its state/power state.

    Args:
        simulator: The simulator handler.

    Raises:
        RuntimeError: In case of the simulation is already running.

    Attributes:
        POWER_STATE_TYPE: A signal that a power state switch ocurred.
        JOB_STARTED_TYPE: A signal that a job started.
        JOB_COMPLETED_TYPE: A signal that a job finished.
    """

    POWER_STATE_TYPE = "p"
    JOB_STARTED_TYPE = "s"
    JOB_COMPLETED_TYPE = "e"

    def __init__(self, simulator: SimulatorHandler) -> None:
        super().__init__(simulator)

        self.__last_state: dict = {}
        self.__info: dict = {
            'time': [], 'energy': [], 'event_type': [], 'wattmin': [], 'epower': []
        }

        self.simulator.subscribe(
            SimulatorEvent.SIMULATION_BEGINS, self.on_simulation_begins)

        self.simulator.subscribe(
            JobEvent.STARTED, self.on_job_started)

        self.simulator.subscribe(
            JobEvent.COMPLETED, self.on_job_completed)

        self.simulator.subscribe(HostEvent.STATE_CHANGED,
                                 self.on_host_state_changed)

        self.simulator.subscribe(
            HostEvent.COMPUTATION_POWER_STATE_CHANGED, self.on_host_state_changed)

    @property
    def info(self) -> dict:
        """ Get monitor info about the simulation.

        Returns:
            A dict with all the information collected.
        """
        return self.__info

    def to_csv(self, fn: str) -> None:
        """ Dump info a csv file. """
        self.to_dataframe().to_csv(fn, index=False)

    def to_dataframe(self) -> pd.DataFrame:
        """ Return a Dataframe object. """
        return pd.DataFrame.from_dict(self.__info)

    def on_simulation_begins(self, sender: SimulatorHandler) -> None:
        self.simulator = sender
        assert self.simulator.platform
        t_now = self.simulator.current_time
        self.__info = {k: [] for k in self.__info.keys()}

        self.__last_state = {
            h.id: (t_now, h.power, h.pstate) for h in self.simulator.platform.hosts
        }

    def on_job_started(self, sender: Job) -> None:
        self.update_info(event_type=self.JOB_STARTED_TYPE)

    def on_job_completed(self, sender: Job) -> None:
        self.update_info(event_type=self.JOB_COMPLETED_TYPE)

    def on_host_state_changed(self, sender: Host) -> None:
        self.update_info(event_type=self.POWER_STATE_TYPE)

    def update_info(self, event_type: str) -> None:
        assert self.simulator and self.simulator.platform
        consumed_energy = wattmin = epower = 0.
        for h in self.simulator.platform.hosts:
            t_start, power, pstate = self.__last_state[h.id]
            t_now = self.simulator.current_time
            wattage = power or 0

            epower += wattage
            consumed_energy += wattage * (t_now - t_start)
            wattmin += pstate.watt_full if pstate else 0

            # Update Last State
            self.__last_state[h.id] = (t_now, h.power, h.pstate)

        consumed_energy += self.__info['energy'][-1] if self.__info['energy'] else 0
        self.__info["time"].append(self.simulator.current_time)
        self.__info["energy"].append(consumed_energy)
        self.__info["event_type"].append(event_type)
        self.__info["wattmin"].append(wattmin)
        self.__info["epower"].append(epower)
