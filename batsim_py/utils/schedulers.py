import math
from copy import copy
from abc import ABC, abstractmethod


from batsim_py.network import SimulatorEventHandler
from batsim_py.rjms import Agenda
import numpy as np


class Scheduler(SimulatorEventHandler):
    def __init__(self, rjms):
        self._rjms = rjms

    @abstractmethod
    def schedule(self):
        raise NotImplementedError()

    def plan(self, *args):
        raise NotImplementedError()


class FirstComeFirstServed(Scheduler):
    def plan(self, queue, resources, agenda):
        plan = []
        if self._rjms.queue_lenght != 0:
            while queue:
                if queue[0].res <= len(resources):
                    job = queue.pop(0)
                    job.expected_time_to_start = 0
                    alloc = [resources.pop(0).id for r in resources[:job.res]]
                    agenda[alloc] = job.walltime
                    plan.append((job, alloc))
                else:
                    break
        return plan

    def schedule(self):
        q = self._rjms.jobs_queue
        r = sorted(self._rjms.get_available_resources(),
                   key=lambda r: r.state.value)
        a = self._rjms.get_reserved_time()
        planning = self.plan(q, r, a)
        for job, alloc in planning:
            self._rjms.allocate(job.id, alloc)


class EASYBackfilling(FirstComeFirstServed):
    def __init__(self, rjms):
        self._priority_job = None
        super().__init__(rjms)

    def plan(self, queue, resources, agenda):
        self._priority_job = None
        queue = list(queue)  # We work on a copy
        plan = super().plan(queue, resources, agenda)

        if len(queue) > 0:  # There is some jobs that could not be scheduled
            # Let's give an upper bound start time for the first job in the queue
            self._priority_job = queue[0]
            earliest_t = sorted(agenda)[:self._priority_job.res]
            earliest_t = math.ceil(earliest_t[-1]) if earliest_t else 0
            self._priority_job.expected_time_to_start = earliest_t

            # Let's backfill some jobs
            plan.extend(self.plan_backfilling(queue[1:], resources, agenda))

        return plan

    def plan_backfilling(self, backfill_queue, resources, agenda):
        assert self._priority_job is not None
        plan = []
        if len(backfill_queue) == 0 or self._priority_job.expected_time_to_start == 0:
            return plan

        #resources = sorted(resources, key=lambda r: r.state.value)
        for job in list(backfill_queue):
            if job.res <= len(resources) and job.walltime <= self._priority_job.expected_time_to_start:
                job.expected_time_to_start = 0
                alloc = [resources.pop(0).id for _ in range(job.res)]
                agenda[alloc] = job.walltime
                plan.append((job, alloc))
                backfill_queue.remove(job)
        return plan


class SAFBackfilling(EASYBackfilling):
    def plan_backfilling(self, backfill_queue, resources, agenda):
        saf_queue = sorted(backfill_queue, key=lambda j: j.walltime * j.res)
        return super().plan_backfilling(saf_queue, resources, agenda)
