import os
import json

from ..network import *
from ..job import Job


def get_profile(data):
    if data['type'] == WorkloadProfileType.delay:
        return DelayProfile(data['delay'])
    elif data['type'] == WorkloadProfileType.parallel:
        return ParallelProfile(data['cpu'], data['com'])
    elif data['type'] == WorkloadProfileType.parallel_homogeneous:
        return ParallelHomogeneousProfile(data['cpu'], data['com'])
    elif data['type'] == WorkloadProfileType.parallel_homogeneous_total:
        return ParallelHomogeneousTotalProfile(data['cpu'], data['com'])
    else:
        raise NotImplementedError


class Workload():
    def __init__(self, name, fn):
        self.name = name
        self.path = fn
        with open(self.path, 'r') as f:
            data = json.load(f)
            self.simulation_time = data.get('simulation_time', None)
            self.profiles = {name: get_profile(
                profile) for name, profile in data['profiles'].items()}
            self.jobs = [Job(
                id="{}!{}".format(self.name, j['id']),
                res=j['res'],
                walltime=j['walltime'],
                profile=j['profile'],
                subtime=j['subtime'],
                user=j.get('user', "")) for j in data['jobs']]
            self.jobs.sort(key=lambda j: j.subtime)


class JobSubmitter(SimulatorEventHandler):
    def __init__(self, simulator):
        super().__init__(simulator)
        self.current_workload = None
        self.workloads = []
        self.current_time = 0
        self.finished = True

    def _load_workload(self):
        assert self.workloads

        w = self.workloads.pop(0)
        w_name = "{}".format(w[w.rfind('/')+1:w.rfind('.json')])
        workload = Workload(w_name, w)

        for profile_name, profile in workload.profiles.items():
            self.simulator.register_profile(
                workload_name=workload.name,
                profile_name=profile_name,
                profile=profile
            )
        # Append workload to current simulation
        if self.current_time > 0:
            for j in workload.jobs:
                j.subtime += self.current_time
        return workload

    def start(self, workloads):
        self.finished = False
        self.current_time = 0
        self.workloads = workloads.copy() if isinstance(
            workloads, list) else [workloads]
        self.current_workload = self._load_workload()
        self.simulator.call_me_later(self.current_workload.jobs[0].subtime)

    def close(self):
        self.finished = True
        self.current_workload = None
        self.workloads = []

    def on_requested_call(self, timestamp, data):
        if self.finished:
            return

        self.current_time = timestamp

        # Check if we need to just wait until simulation time.
        if len(self.current_workload.jobs) == 0 and self.current_workload.simulation_time and self.current_time < self.current_workload.simulation_time:
            return
        # Check if there is a job to be submitted
        elif len(self.current_workload.jobs) > 0 and self.current_time < self.current_workload.jobs[0].subtime:
            return

        while self.current_workload.jobs and timestamp >= self.current_workload.jobs[0].subtime:
            job = self.current_workload.jobs.pop(0)
            self.simulator.register_job(
                job.id,
                job.profile,
                job.res,
                job.walltime,
                job.user
            )

        # Check if there is job to be submitted or not
        if len(self.current_workload.jobs) == 0:
            # Check if we need to wait for the simulation time.
            if self.current_workload.simulation_time and self.current_time < self.current_workload.simulation_time:
                self.simulator.call_me_later(
                    self.current_workload.simulation_time)
            else:
                self.close()
                self.simulator.notify(NotifyType.registration_finished)
        else:
            self.simulator.call_me_later(self.current_workload.jobs[0].subtime)
