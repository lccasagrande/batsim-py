
import time as tm
from abc import abstractmethod, ABC
from copy import copy
from collections import defaultdict
from itertools import groupby

import pandas as pd
from procset import ProcSet
from pydispatch import dispatcher

from .jobs import JobState
from .resources import HostState
from .resources import PowerStateType
from .events import SimulatorEvent
from .events import HostEvent
from .events import JobEvent


class Monitor(ABC):
    def __init__(self, simulator):
        self.__simulator = simulator

    @property
    def simulator(self):
        return self.__simulator

    @property
    @abstractmethod
    def info(self):
        raise NotImplementedError

    @abstractmethod
    def to_csv(self, fn):
        raise NotImplementedError


class JobMonitor(Monitor):
    def __init__(self, simulator):
        super().__init__(simulator)
        self.__info = {
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

        dispatcher.connect(
            self.__on_simulation_begins,
            signal=SimulatorEvent.SIMULATION_BEGINS,
            sender=self.simulator
        )

        dispatcher.connect(
            self.__update_info,
            signal=JobEvent.COMPLETED,
            sender=dispatcher.Any
        )

        dispatcher.connect(
            self.__update_info,
            signal=JobEvent.KILLED,
            sender=dispatcher.Any
        )

        dispatcher.connect(
            self.__update_info,
            signal=JobEvent.REJECTED,
            sender=dispatcher.Any
        )

    @property
    def info(self):
        return self.__info

    def to_csv(self, fn):
        pd.DataFrame.from_dict(self.__info).to_csv(fn, index=False)

    def __on_simulation_begins(self, sender):
        self.__info = {k: [] for k in self.__info.keys()}

    def __update_info(self, sender):
        self.__info['job_id'].append(sender.id)
        self.__info['workload_name'].append(sender.workload)
        self.__info['profile'].append(sender.profile.name)
        self.__info['submission_time'].append(sender.subtime)
        self.__info['requested_number_of_resources'].append(sender.res)
        self.__info['requested_time'].append(sender.walltime)
        self.__info['success'].append(
            int(sender.state == JobState.COMPLETED_SUCCESSFULLY))
        self.__info['final_state'].append(str(sender.state))
        self.__info['starting_time'].append(sender.start_time)
        self.__info['execution_time'].append(sender.runtime)
        self.__info['finish_time'].append(sender.stop_time)
        self.__info['waiting_time'].append(sender.waiting_time)
        self.__info['turnaround_time'].append(sender.turnaround_time)
        self.__info['stretch'].append(sender.slowdown)
        self.__info['allocated_resources'].append(
            str(ProcSet(*sender.allocation)))
        self.__info['consumed_energy'].append(-1)


class SchedulerMonitor(Monitor):
    def __init__(self, simulator):
        super().__init__(simulator)
        self.__info = {
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

        dispatcher.connect(
            self.__on_simulation_begins,
            signal=SimulatorEvent.SIMULATION_BEGINS,
            sender=self.simulator
        )

        dispatcher.connect(
            self.__on_simulation_ends,
            signal=SimulatorEvent.SIMULATION_ENDS,
            sender=self.simulator
        )

        dispatcher.connect(
            self.__on_job_completed,
            signal=JobEvent.COMPLETED,
            sender=dispatcher.Any
        )

        dispatcher.connect(
            self.__on_job_completed,
            signal=JobEvent.KILLED,
            sender=dispatcher.Any
        )

        dispatcher.connect(
            self.__on_job_submitted,
            signal=JobEvent.SUBMITTED,
            sender=dispatcher.Any
        )

        dispatcher.connect(
            self.__on_job_rejected,
            signal=JobEvent.REJECTED,
            sender=dispatcher.Any
        )

    @property
    def info(self):
        return self.__info

    def to_csv(self, fn):
        pd.DataFrame.from_dict(
            self.__info, orient='index').T.to_csv(fn, index=False)

    def __on_simulation_begins(self, sender):
        self.__info = {k: 0 for k in self.__info.keys()}

    def __on_simulation_ends(self, sender):
        self.__info['makespan'] = sender.current_time
        nb_finished = max(1, self.__info['nb_jobs_finished'])
        self.__info['mean_waiting_time'] /= nb_finished
        self.__info['mean_slowdown'] /= nb_finished
        self.__info['mean_pp_slowdown'] /= nb_finished
        self.__info['mean_stretch'] /= nb_finished
        self.__info['mean_turnaround_time'] /= nb_finished

    def __on_job_submitted(self, sender):
        self.__info['nb_jobs'] += 1

    def __on_job_rejected(self, sender):
        self.__info['nb_jobs_rejected'] += 1

    def __on_job_completed(self, sender):
        if not sender.is_finished:
            return

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
    def __init__(self, simulator):
        super().__init__(simulator)
        self.__last_host_state = None
        self.__info = {
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

        dispatcher.connect(
            self.__on_simulation_begins,
            signal=SimulatorEvent.SIMULATION_BEGINS,
            sender=self.simulator
        )
        dispatcher.connect(
            self.__on_simulation_ends,
            signal=SimulatorEvent.SIMULATION_ENDS,
            sender=self.simulator
        )

        dispatcher.connect(
            self.__on_host_state_changed,
            signal=HostEvent.STATE_CHANGED,
            sender=dispatcher.Any
        )
        dispatcher.connect(
            self.__on_host_state_changed,
            signal=HostEvent.COMPUTATION_POWER_STATE_CHANGED,
            sender=dispatcher.Any
        )

    def to_csv(self, fn):
        pd.DataFrame.from_dict(
            self.__info, orient='index').T.to_csv(fn, index=False)

    @property
    def info(self):
        return self.__info

    def __on_simulation_begins(self, sender):
        self.__last_host_state = {
            h.id: (h.pstate, h.state, self.simulator.current_time) for h in self.simulator.platform.hosts
        }

        self.__info = {k: 0 for k in self.__info.keys()}
        self.__info['nb_computing_machines'] = self.simulator.platform.size

    def __on_simulation_ends(self, sender):
        for h in self.simulator.platform.hosts:
            self.__update_info(h)


    def __on_host_state_changed(self, sender):
        self.__update_info(sender)

    def __update_info(self, host):
        pstate, state, tstart = self.__last_host_state[host.id]

        # Update Info
        time_spent = self.simulator.current_time - tstart
        if state == HostState.IDLE:
            self.__info['time_idle'] += time_spent
            consumption = waste = pstate.power_profile.idle * time_spent
        elif state == HostState.COMPUTING:
            self.__info['time_computing'] += time_spent
            consumption, waste = pstate.power_profile.full_load * time_spent, 0
        elif state == HostState.SWITCHING_OFF:
            self.__info['time_switching_off'] += time_spent
            consumption = waste = pstate.power_profile.idle * time_spent
        elif state == HostState.SWITCHING_ON:
            self.__info['time_switching_on'] += time_spent
            consumption = waste = pstate.power_profile.idle * time_spent
        elif state == HostState.SLEEPING:
            self.__info['time_sleeping'] += time_spent
            consumption, waste = pstate.power_profile.idle * time_spent, 0
        else:
            raise NotImplementedError

        self.__info['consumed_joules'] += consumption
        self.__info['energy_waste'] += waste

        if pstate.id != host.pstate.id:
            self.__info['nb_switches'] += 1

        # Update Last State
        self.__last_host_state[host.id] = (
            host.pstate, host.state, self.simulator.current_time)


class SimulationMonitor(Monitor):
    def __init__(self, simulator):
        super().__init__(simulator)
        self.__sched_monitor = SchedulerMonitor(simulator)
        self.__hosts_monitor = HostMonitor(simulator)
        self.__sim_start_time = self.__simulation_time = -1

        dispatcher.connect(
            self.__on_simulation_begins,
            signal=SimulatorEvent.SIMULATION_BEGINS,
            sender=self.simulator
        )

        dispatcher.connect(
            self.__on_simulation_ends,
            signal=SimulatorEvent.SIMULATION_ENDS,
            sender=self.simulator
        )

    @property
    def info(self):
        info = dict(self.__sched_monitor.info, **self.__hosts_monitor.info)
        info['simulation_time'] = self.__simulation_time
        return info

    def __on_simulation_begins(self, sender):
        self.__sim_start_time = tm.time()

    def __on_simulation_ends(self, sender):
        self.__simulation_time = tm.time() - self.__sim_start_time

    def to_csv(self, fn):
        pd.DataFrame.from_dict(
            self.info, orient='index').T.to_csv(fn, index=False)


class HostStateSwitchMonitor(Monitor):
    def __init__(self, simulator):
        super().__init__(simulator)
        self.__last_host_state = None
        self.__info = {
            'time': [],
            'nb_sleeping': [],
            'nb_switching_on': [],
            'nb_switching_off': [],
            'nb_idle': [],
            'nb_computing': []
        }
        dispatcher.connect(
            self.__on_simulation_begins,
            signal=SimulatorEvent.SIMULATION_BEGINS,
            sender=self.simulator
        )

        dispatcher.connect(
            self.__on_host_state_changed,
            signal=HostEvent.STATE_CHANGED,
            sender=dispatcher.Any
        )

    @property
    def info(self):
        return self.__info

    def to_csv(self, fn):
        pd.DataFrame.from_dict(self.__info).to_csv(fn, index=False)

    def __on_simulation_begins(self, sender):
        self.__last_host_state = {}
        self.__info = {k: [0] for k in self.__info.keys()}

        # Update initial state
        self.__info['time'][-1] = self.simulator.current_time
        for h in self.simulator.platform.hosts:
            key = self.__get_key_from_state(h.state)
            self.__last_host_state[h.id] = key
            self.__info[key][-1] += 1

    def __on_host_state_changed(self, sender):
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

    def __get_key_from_state(self, state):
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
    def __init__(self, simulator):
        super().__init__(simulator)

        self.__last_host_pstate_id = None
        self.__info = {'time': [], 'machine_id': [], 'new_pstate': []}

        dispatcher.connect(
            self.__on_simulation_begins,
            signal=SimulatorEvent.SIMULATION_BEGINS,
            sender=self.simulator
        )

        dispatcher.connect(
            self.__on_host_power_state_changed,
            signal=HostEvent.COMPUTATION_POWER_STATE_CHANGED,
            sender=dispatcher.Any
        )
        dispatcher.connect(
            self.__on_host_power_state_changed,
            signal=HostEvent.STATE_CHANGED,
            sender=dispatcher.Any
        )

    @property
    def info(self):
        return self.__info

    def to_csv(self, fn):
        pd.DataFrame.from_dict(self.__info).to_csv(fn, index=False)

    def __on_simulation_begins(self, sender):
        self.__last_host_pstate_id = {
            h.id: h.pstate.id for h in self.simulator.platform.hosts
        }
        self.__info = {k: [] for k in self.__info.keys()}

        # Record initial state
        for pstate_id, hosts in groupby(self.simulator.platform.hosts, key=lambda h: h.pstate.id):
            self.__info['time'].append(self.simulator.current_time)
            procset = ProcSet(*[h.id for h in hosts])
            self.__info['machine_id'].append(str(procset))
            self.__info['new_pstate'].append(pstate_id)

    def __on_host_power_state_changed(self, sender):
        if self.__last_host_pstate_id[sender.id] == sender.pstate.id:
            return

        self.__last_host_pstate_id[sender.id] = sender.pstate.id
        if sender.pstate.type == PowerStateType.SWITCHING_OFF:
            new_pstate_id = -2
        elif sender.pstate.type == PowerStateType.SWITCHING_ON:
            new_pstate_id = -1
        else:
            new_pstate_id = sender.pstate.id

        if self.__info['time'][-1] == self.simulator.current_time and self.__info['new_pstate'][-1] == new_pstate_id:
            # Update last recorda
            procset = ProcSet.from_str(self.__info['machine_id'][-1])
            procset.update(ProcSet(sender.id))
            self.__info['machine_id'][-1] = str(procset)
        else:
            # Record new state
            self.__info['time'].append(self.simulator.current_time)
            self.__info['machine_id'].append(str(sender.id))
            self.__info['new_pstate'].append(new_pstate_id)


class ConsumedEnergyMonitor(Monitor):
    POWER_STATE_TYPE = "p"
    JOB_STARTED_TYPE = "s"
    JOB_COMPLETED_TYPE = "e"

    def __init__(self, simulator):
        super().__init__(simulator)
        self.__info = {
            'time': [], 'energy': [], 'event_type': [], 'wattmin': [], 'epower': []
        }
        self.__last_host_state = None

        dispatcher.connect(
            self.__on_simulation_begins,
            signal=SimulatorEvent.SIMULATION_BEGINS,
            sender=self.simulator
        )

        dispatcher.connect(
            self.__handle_job_started_event,
            signal=JobEvent.STARTED,
            sender=dispatcher.Any)

        dispatcher.connect(
            self.__handle_job_completed_event,
            signal=JobEvent.COMPLETED,
            sender=dispatcher.Any
        )

        dispatcher.connect(
            self.__handle_job_completed_event,
            signal=JobEvent.KILLED,
            sender=dispatcher.Any
        )

        dispatcher.connect(
            self.__handle_host_event,
            signal=HostEvent.STATE_CHANGED,
            sender=dispatcher.Any
        )
        dispatcher.connect(
            self.__handle_host_event,
            signal=HostEvent.COMPUTATION_POWER_STATE_CHANGED,
            sender=dispatcher.Any
        )

    @property
    def info(self):
        return self.__info

    def to_csv(self, fn):
        pd.DataFrame.from_dict(self.__info).to_csv(fn, index=False)

    def __on_simulation_begins(self, sender):
        self.__info = {k: [] for k in self.__info.keys()}
        self.__last_host_state = {
            h.id: (h.pstate, h.state, self.simulator.current_time) for h in self.simulator.platform.hosts
        }

    def __handle_job_started_event(self, sender):
        self.__update_info(event_type=self.JOB_STARTED_TYPE)

    def __handle_job_completed_event(self, sender):
        self.__update_info(event_type=self.JOB_COMPLETED_TYPE)

    def __handle_host_event(self, sender):
        self.__update_info(event_type=self.POWER_STATE_TYPE)

    def __update_info(self, event_type):
        consumed_energy = wattmin = epower = 0
        for host in self.simulator.platform.hosts:
            pstate, state, tstart = self.__last_host_state[host.id]

            time = self.simulator.current_time - tstart
            wattage = pstate.power_profile.full_load if state == HostState.COMPUTING else pstate.power_profile.idle

            epower += wattage
            consumed_energy += wattage * time
            wattmin += host.pstate.power_profile.full_load

            # Update Last State
            self.__last_host_state[host.id] = (
                host.pstate, host.state, self.simulator.current_time)

        # if self.__info['time'] and self.__info['time'][-1] == self.simulator.current_time and self.__info['event_type'][-1] == event_type:
        #    self.__info['energy'][-1] = consumed_energy
        #    self.__info['wattmin'][-1] = wattmin
        #    self.__info['epower'][-1] = epower
        # else:
        consumed_energy += self.__info['energy'][-1] if self.__info['energy'] else 0
        self.__info["time"].append(self.simulator.current_time)
        self.__info["energy"].append(consumed_energy)
        self.__info["event_type"].append(event_type)
        self.__info["wattmin"].append(wattmin)
        self.__info["epower"].append(epower)
