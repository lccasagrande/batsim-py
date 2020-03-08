import math
from copy import copy
from abc import ABC, abstractmethod


from batsim_py.network import SimulatorEventHandler
from batsim_py.rjms import Agenda
import numpy as np


class Scheduler():
    @abstractmethod
    def schedule(self, *args):
        raise NotImplementedError()


class FirstComeFirstServed(Scheduler):
    def schedule(self, queue, resources):
        jobs = []
        for job in queue:
            if job.res <= len(resources):
                jobs.append(job)
                job.expected_time_to_start = 0
            else:
                break
        return jobs


class EASYBackfilling(Scheduler):
    def __init__(self):
        super().__init__()
        self._priority_job = None

    def schedule(self, queue, resources, agenda):
        self._priority_job = None
        jobs, resources, queue, agenda = [], list(resources), list(queue), np.array(agenda)

        for job in list(queue):
            if job.res <= len(resources):
                alloc = [resources.pop(0).id for r in resources[:job.res]]
                agenda[alloc] = job.walltime
                job.expected_time_to_start = 0
                jobs.append(job)
                queue.remove(job)
            else:
                break
        # We need this to backfill jobs
        if len(queue) > 0:  # There is some jobs that could not be scheduled
            # Let's give an upper bound start time for the first job in the queue
            self._priority_job = queue[0]
            earliest_t = sorted(agenda)[:self._priority_job.res]
            earliest_t = math.ceil(earliest_t[-1]) if earliest_t else 0
            self._priority_job.expected_time_to_start = earliest_t

            # Let's backfill some jobs
            jobs.extend(self.backfill(queue[1:], resources))

        return jobs

    def backfill(self, backfill_queue, resources):
        assert self._priority_job is not None
        jobs = []
        if self._priority_job.expected_time_to_start != 0:
            for job in backfill_queue:
                if job.res <= len(resources) and job.walltime <= self._priority_job.expected_time_to_start:
                    job.expected_time_to_start = 0
                    del resources[:job.res]
                    jobs.append(job)
        return jobs


class SAFBackfilling(EASYBackfilling):
    def backfill(self, backfill_queue, resources):
        saf_queue = sorted(backfill_queue, key=lambda j: j.walltime * j.res)
        return super().backfill(saf_queue, resources)
