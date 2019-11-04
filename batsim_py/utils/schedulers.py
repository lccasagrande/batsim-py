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


class FirstComeFirstServed(Scheduler):
    def schedule(self):
        if self._rjms.queue_lenght == 0:
            return

        available = self._rjms.get_available_resources()
        available = sorted(available, key=lambda r: r.state.value)
        for job in self._rjms.jobs_queue:
            if job.res <= len(available):
                job.expected_time_to_start = 0
                alloc = [r.id for r in available[:job.res]]
                self._rjms.allocate(job.id, alloc)
                del available[:job.res]
            else:
                break


class EASYBackfilling(FirstComeFirstServed):
    def __init__(self, rjms):
        self._priority_job = None
        super().__init__(rjms)

    def _backfill(self, backfill_queue):
        assert self._priority_job is not None
        if len(backfill_queue) == 0 or self._priority_job.expected_time_to_start == 0:
            return

        available = self._rjms.get_available_resources()
        available = sorted(available, key=lambda r: r.state.value)
        for job in backfill_queue:
            if job.res <= len(available) and job.walltime <= self._priority_job.expected_time_to_start:
                job.expected_time_to_start = 0
                alloc = [r.id for r in available[:job.res]]
                self._rjms.allocate(job.id, alloc)
                del available[:job.res]

    def schedule(self):
        def estimate_start_time(job):
            earliest_time = sorted(self._rjms.get_reserved_time())[:job.res]
            return math.ceil(earliest_time[-1]) if earliest_time else 0

        super().schedule()
        self._priority_job = None
        if self._rjms.queue_lenght > 0:  # There is some jobs that could not be scheduled
            # Let's give an upper bound start time for the first job in the queue
            queue = self._rjms.jobs_queue
            self._priority_job = queue[0]
            self._priority_job.expected_time_to_start = estimate_start_time(
                self._priority_job)

            # Let's backfill some jobs
            self._backfill(queue[1:])


class SAFBackfilling(EASYBackfilling):
    def _backfill(self, backfill_queue):
        saf_queue = sorted(backfill_queue, key=lambda j: j.walltime * j.res)
        super()._backfill(saf_queue)
