import time as tm
from abc import abstractmethod
from copy import copy
from itertools import groupby

import numpy as np
from procset import ProcSet

from batsim_py.job import Job, JobState
from batsim_py.resource import ResourceState, PowerStateType
from batsim_py.network import SimulatorEventHandler


class Timeout(SimulatorEventHandler):
    def __init__(self, idling_time, rjms):
        assert idling_time > 0
        self.idling_time = idling_time
        self.next_call = None
        self.rjms = rjms
        self._idle_nodes = {}
        super().__init__(rjms.simulator)

    def get_next_call(self):
        if len(self._idle_nodes) == 0:
            return None

        t = min(t for t in list(self._idle_nodes.values()))
        return t + self.idling_time

    def set_callback(self):
        next_call = self.get_next_call()
        if next_call and (not self.next_call or next_call < self.next_call):
            self.next_call = next_call
            self.simulator.call_me_later(self.next_call)

    def on_requested_call(self, timestamp, data):
        if self.next_call and timestamp == self.next_call:
            self.next_call = None

        for node_id in list(self._idle_nodes.keys()):
            time_idle = timestamp - self._idle_nodes[node_id]
            if time_idle == self.idling_time:
                self.rjms.turn_off(node_id)
                del self._idle_nodes[node_id]
            elif time_idle > self.idling_time:
                raise RuntimeError(
                    "Resource is idle for more time than allowed (idling time)")
        self.set_callback()

    def on_simulation_begins(self, timestamp, data):
        self._idle_nodes = {
            n.id: timestamp for n in self.rjms.platform.nodes if n.is_idle
        }
        self.set_callback()

    def on_job_started(self, timestamp, data):
        # Resources are allocated
        for r in self.rjms.platform.get_resources(data.alloc):
            self._idle_nodes.pop(r.parent_id, None)

    def on_job_completed(self, timestamp, data):
        # Resources are released
        for r in self.rjms.platform.get_resources(data.alloc):
            self._idle_nodes[r.parent_id] = timestamp
        self.set_callback()

    def on_job_killed(self, timestamp, data):
        # Resources are released
        for node in self.rjms.platform.nodes:
            if node.is_idle:
                self._idle_nodes.setdefault(node.id, timestamp)
        self.set_callback()

    def on_resource_power_state_changed(self, timestamp, data):
        for node in self.rjms.platform.nodes:
            if node.is_idle:
                self._idle_nodes.setdefault(node.id, timestamp)
            else:
                self._idle_nodes.pop(node.id, None)
        self.set_callback()
