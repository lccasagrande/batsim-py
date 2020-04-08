import json
from abc import ABC, abstractmethod
from enum import Enum

from pydispatch import dispatcher

from .utils.commons import Identifier
from .events import JobEvent


class JobProfileType(Enum):
    """ Batsim Workload Profiles """
    DELAY = 0
    PARALLEL = 1
    PARALLEL_HOMOGENEOUS = 2
    PARALLEL_HOMOGENEOUS_TOTAL = 3
    COMPOSED = 4
    PARALLEL_HOMOGENEOUS_PFS = 5
    DATA_STAGING = 6

    def __str__(self):
        return self.name


class JobState(Enum):
    UNKNOWN = 0
    SUBMITTED = 1
    RUNNING = 2
    COMPLETED_SUCCESSFULLY = 3
    COMPLETED_FAILED = 4
    COMPLETED_WALLTIME_REACHED = 5
    COMPLETED_KILLED = 6
    REJECTED = 7
    ALLOCATED = 8

    def __str__(self):
        return self.name


class JobProfile(ABC):
    def __init__(self, name, profile_type):
        assert isinstance(profile_type, JobProfileType)
        self.name = name
        self.type = profile_type


class DelayJobProfile(JobProfile):
    def __init__(self, name, delay):
        super().__init__(name, JobProfileType.DELAY)
        self.delay = delay


class ParallelJobProfile(JobProfile):
    def __init__(self, name, cpu, com):
        super().__init__(name, JobProfileType.PARALLEL)
        assert isinstance(cpu, list)
        assert isinstance(com, list)
        assert len(com) == len(cpu) * \
            len(cpu), "The communication matrix must be = [host x host]"
        self.cpu = cpu
        self.com = com


class ParallelHomogeneousJobProfile(JobProfile):
    def __init__(self, name, cpu, com):
        super().__init__(name, JobProfileType.PARALLEL_HOMOGENEOUS)
        self.cpu = float(cpu)
        self.com = float(com)


class ParallelHomogeneousTotalJobProfile(JobProfile):
    def __init__(self, name, cpu, com):
        super().__init__(name, JobProfileType.PARALLEL_HOMOGENEOUS_TOTAL)
        self.cpu = float(cpu)
        self.com = float(com)


class ComposedJobProfile(JobProfile):
    def __init__(self, name, profiles, repeat=1):
        super().__init__(name, JobProfileType.COMPOSED)
        assert repeat > 0
        assert len(
            profiles) > 1, "A composed profile must have at least 2 profiles."
        assert all(isinstance(p, JobProfile) for p in profiles)

        self.repeat = repeat
        self.profiles = profiles


class ParallelHomogeneousPFSJobProfile(JobProfile):
    def __init__(self, name, bytes_to_read, bytes_to_write, storage='pfs'):
        super().__init__(name, JobProfileType.PARALLEL_HOMOGENEOUS_PFS)
        assert bytes_to_read > 0
        assert bytes_to_write > 0
        assert storage

        self.bytes_to_read = bytes_to_read
        self.bytes_to_write = bytes_to_write
        self.storage = storage


class DataStagingJobProfile(JobProfile):
    def __init__(self, name, nb_bytes, src, dest):
        super().__init__(name, JobProfileType.DATA_STAGING)
        assert nb_bytes > 0
        self.nb_bytes = nb_bytes
        self.src = src
        self.dest = dest


class Job(Identifier):
    WORKLOAD_SEPARATOR = "!"

    def __init__(self, name, workload, res, profile, subtime, walltime=None, user=None):
        super().__init__("%s%s%s" % (str(workload), self.WORKLOAD_SEPARATOR, str(name)))
        assert isinstance(profile, JobProfile)

        self.__res = res
        self.__profile = profile
        self.__subtime = subtime
        self.__walltime = walltime
        self.__user = user

        self.__state = JobState.UNKNOWN
        self.__allocation = []  # will be set on scheduling
        self.__start_time = None  # will be set on start
        self.__stop_time = None  # will be set on terminate
        self.metadata = {}

    def __repr__(self):
        return "Job_%s" % self.id

    @property
    def name(self):
        return self.id.split(self.WORKLOAD_SEPARATOR)[1]

    @property
    def workload(self):
        return self.id.split(self.WORKLOAD_SEPARATOR)[0]

    @property
    def subtime(self):
        return self.__subtime

    @property
    def res(self):
        return self.__res

    @property
    def profile(self):
        return self.__profile

    @property
    def walltime(self):
        return self.__walltime

    @property
    def user(self):
        return self.__user

    @property
    def state(self):
        return self.__state

    @property
    def allocation(self):
        return list(self.__allocation)

    @property
    def is_running(self):
        return self.__state == JobState.RUNNING

    @property
    def is_runnable(self):
        return self.__state == JobState.ALLOCATED

    @property
    def is_submitted(self):
        return self.__state == JobState.SUBMITTED

    @property
    def is_finished(self):
        return self.stop_time != None

    @property
    def start_time(self):
        return self.__start_time

    @property
    def stop_time(self):
        return self.__stop_time

    @property
    def dependencies(self):
        return None

    @property
    def stretch(self):
        if self.walltime:
            return self.waiting_time / self.walltime if self.start_time != None else None
        else:
            return self.waiting_time / self.runtime if self.runtime else None

    @property
    def waiting_time(self):
        return self.start_time - self.subtime if self.start_time != None else None

    @property
    def runtime(self):
        return self.stop_time - self.start_time if self.is_finished else None

    @property
    def turnaround_time(self):
        return self.waiting_time + self.runtime if self.is_finished else None

    @property
    def per_processor_slowdown(self):
        return max(1, self.turnaround_time / (self.res * self.runtime)) if self.is_finished else None

    @property
    def slowdown(self):
        return max(1, self.turnaround_time / self.runtime) if self.is_finished else None

    def _allocate(self, hosts):
        assert not self.__allocation, "Cannot change job allocation."
        assert len(hosts) == self.res, "Insufficient resources."
        self.__allocation = list(hosts)
        self.__state = JobState.ALLOCATED
        self.__dispatch(JobEvent.ALLOCATED)

    def _reject(self):
        self.__state == JobState.REJECTED
        self.__dispatch(JobEvent.REJECTED)

    def _submit(self, subtime):
        assert self.state == JobState.UNKNOWN
        assert subtime >= 0
        self.__state = JobState.SUBMITTED
        self.__subtime = subtime
        self.__dispatch(JobEvent.SUBMITTED)

    def _kill(self, current_time):
        assert self.is_running, "A job must be running to be able to kill it."
        assert current_time >= self.start_time
        self.__stop_time = current_time
        self.__state = JobState.COMPLETED_KILLED
        self.__dispatch(JobEvent.KILLED)

    def _start(self, current_time):
        assert self.start_time is None, "Job already started."
        assert self.state == JobState.ALLOCATED, "A job cannot start without an allocation."
        assert current_time >= self.__subtime
        self.__start_time = current_time
        self.__state = JobState.RUNNING
        self.__dispatch(JobEvent.STARTED)

    def _terminate(self, current_time, state):
        assert self.is_running, "A job must be running to be able to terminate."
        assert state == JobState.COMPLETED_SUCCESSFULLY or state == JobState.COMPLETED_FAILED or state == JobState.COMPLETED_WALLTIME_REACHED
        assert current_time >= self.start_time
        self.__stop_time = current_time
        self.__state = state
        self.__dispatch(JobEvent.COMPLETED)

    def __dispatch(self, event_type):
        assert isinstance(event_type, JobEvent)
        dispatcher.send(signal=event_type, sender=self)
        listeners = list(dispatcher.liveReceivers(
            dispatcher.getReceivers(self, event_type)))
        for r in list(listeners):
            dispatcher.disconnect(r, signal=event_type, sender=self)
