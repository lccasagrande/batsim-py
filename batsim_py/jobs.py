from abc import ABC
from enum import Enum
from typing import Optional
from typing import Sequence
from typing import Union

from . import dispatcher
from .events import JobEvent
from .utils import Identifier


class JobState(Enum):
    """ Batsim Job State

        This class enumerates the distinct states a job can be in.
    """

    UNKNOWN = 0
    SUBMITTED = 1
    RUNNING = 2
    COMPLETED_SUCCESSFULLY = 3
    COMPLETED_FAILED = 4
    COMPLETED_WALLTIME_REACHED = 5
    COMPLETED_KILLED = 6
    REJECTED = 7
    ALLOCATED = 8

    def __str__(self) -> str:
        return self.name


class JobProfile(ABC):
    """ Batsim Job Profile base class.

    This is the base class for the different profiles a job can execute.

    Args:
        name: The profile name. Must be unique within a workload.
    """

    def __init__(self, name: str) -> None:
        self.__name = str(name)

    @property
    def name(self) -> str:
        """The profile's name. """
        return self.__name


class DelayJobProfile(JobProfile):
    """ Batsim Delay Job Profile class.

    This class describes a job that will sleep during the defined time.

    Args:
        name: The profile name. Must be unique within a workload.
        delay: The seconds to sleep.

    Raises:
        ValueError: In case of delay is not greater than zero.

    Examples:
        A job that will sleep for 100 seconds.

        >>> profile = DelayJobProfile(name="delay", delay=100)
    """

    def __init__(self, name: str, delay: Union[int, float]) -> None:
        if delay <= 0:
            raise ValueError('Expected `delay` argument to be a number '
                             'greater than zero, got {}'.format(delay))

        super().__init__(name)
        self.__delay = float(delay)

    @property
    def delay(self) -> float:
        """The time which the job will sleep. """
        return self.__delay


class ParallelJobProfile(JobProfile):
    """ Batsim Parallel Job Profile class.

    This class describes a job that performs a set of computations
    and communications on allocated hosts.

    Args:
        name: The profile name. Must be unique within a workload.
        cpu: A list that defines the amount of flop/s to be 
            computed on each allocated host.
        com: A list [host x host] that defines the amount of bytes 
            to be transferred between allocated hosts.

    Raises:
        ValueError: In case of `com` argument invalid size or `cpu` is empty
            or values are less than zero.

    Examples:
        Two hosts computing 10E6 flop/s each with local communication only.

        >>> profile = ParallelJobProfile(name="parallel", cpu=[10e6, 10e6], com=[5e6, 0, 0, 5e6])

        Two hosts computing 2E6 flop/s each with host 1 sending 5E6 bytes to host 2.

        >>> profile = ParallelJobProfile(name="parallel", cpu=[2e6, 2e6], com=[0, 5e6, 0, 0])

        One host computing 2E6 flop/s with host 1 sending 5E6 bytes to host 2 and host 2 sending 10E6 bytes to host 1.

        >>> profile = ParallelJobProfile(name="parallel", cpu=[2e6, 0], com=[0, 5e6, 10e6, 0])
    """

    def __init__(self,
                 name: str,
                 cpu: Sequence[Union[int, float]],
                 com: Sequence[Union[int, float]]) -> None:
        if not cpu:
            raise ValueError('Expected `cpu` argument to be a non '
                             'empty sequence, got {}.'.format(cpu))

        if len(com) != len(cpu) * len(cpu):
            raise ValueError('Expected `com` argument to be a '
                             'list of size [host x host], got {}'.format(com))

        if not all(c >= 0 for c in cpu):
            raise ValueError('Expected `cpu` argument to have values '
                             'not less than zero.')

        if not all(c >= 0 for c in com):
            raise ValueError('Expected `com` argument to have values '
                             'not less than zero.')

        super().__init__(name)
        self.__cpu = list(cpu)
        self.__com = list(com)

    @property
    def cpu(self) -> Sequence[Union[int, float]]:
        """The amount of flop/s to be computed on each host."""
        return self.__cpu

    @property
    def com(self) -> Sequence[Union[int, float]]:
        """The amount of bytes to be transferred between hosts."""
        return self.__com


class ParallelHomogeneousJobProfile(JobProfile):
    """ Batsim Parallel Homogeneous Job Profile class.

    This class describes a job that performs the same computation
    and communication on all allocated hosts.

    Args:
        name: The profile name. Must be unique within a workload.
        cpu: The amount of flop/s to be computed on each allocated host.
        com: The amount of bytes to send and receive between each pair of hosts.
            The loopback communication of each machine defaults to 0.

    Raises:
        ValueError: In case of both `com` and `cpu` arguments are not greater
            than zero or any of them is negative.

    Examples:
        >>> profile = ParallelHomogeneousJobProfile("name", cpu=10e6, com=5e6)
    """

    def __init__(self,
                 name: str,
                 cpu: Union[int, float],
                 com: Union[int, float]) -> None:
        if cpu == 0 and com == 0:
            raise ValueError('Expected `cpu` or `com` arguments '
                             ' to be greater than zero.')
        if cpu < 0:
            raise ValueError('Expected `cpu` argument to be '
                             'not less than zero.')
        if com < 0:
            raise ValueError('Expected `cpu` argument to be '
                             'not less than zero.')

        super().__init__(name)
        self.__cpu = float(cpu)
        self.__com = float(com)

    @property
    def cpu(self) -> float:
        """The amount of flop/s to be computed on each host."""
        return self.__cpu

    @property
    def com(self) -> float:
        """The amount of bytes to be transferred between hosts."""
        return self.__com


class ParallelHomogeneousTotalJobProfile(JobProfile):
    """ Batsim Parallel Homogeneous Total Job Profile class.

    This class describes a job that equally distributes the total
    amount of computation and communication between the allocated hosts.

    Args:
        name: The profile name. Must be unique within a workload.
        cpu: The total amount of flop/s to be computed over all hosts.
        com: The total amount of bytes to be sent and received on each
            pair of hosts.

    Raises:
        ValueError: In case of both `com` and `cpu` arguments are not greater
            than zero or any of them is negative.

    Examples:
        >>> profile = ParallelHomogeneousTotalJobProfile("name", cpu=10e6, com=5e6)
    """

    def __init__(self,
                 name: str,
                 cpu:  Union[int, float],
                 com:  Union[int, float]) -> None:
        if cpu == 0 and com == 0:
            raise ValueError('Expected `cpu` or `com` arguments '
                             ' to be greater than zero.')
        if cpu < 0:
            raise ValueError('Expected `cpu` argument to be '
                             'not less than zero.')
        if com < 0:
            raise ValueError('Expected `cpu` argument to be '
                             'not less than zero.')

        super().__init__(name)
        self.__cpu = float(cpu)
        self.__com = float(com)

    @property
    def cpu(self) -> float:
        """The total amount of flop/s to be computed."""
        return self.__cpu

    @property
    def com(self) -> float:
        """The total amount of bytes to be sent and received."""
        return self.__com


class ComposedJobProfile(JobProfile):
    """ Batsim Composed Job Profile class.

    This class describes a job that executes a sequence of profiles.

    Args:
        name: The profile name. Must be unique within a workload.
        profiles: The profiles to execute.
        repeat: The number of times to repeat the sequence.

    Raises:
        TypeError: In case of invalid arguments.
        ValueError: In case of `repeat` argument is less than 1 or profiles
            is empty.

    Examples:
        >>> profile_1 = ParallelHomogeneousTotalJobProfile("prof1", cpu=10e6, com=5e6)
        >>> profile_2 = ParallelHomogeneousTotalJobProfile("prof2", cpu=1e6, com=2e6)
        >>> composed = ComposedJobProfile("composed", profiles=[profile_1, profile_2], repeat=2)
    """

    def __init__(self,
                 name: str,
                 profiles: Sequence[JobProfile],
                 repeat: int = 1) -> None:
        if repeat <= 0:
            raise ValueError('Expected `repeat` argument to be greater'
                             'than 0, got {}'.format(repeat))

        if not profiles:
            raise ValueError('Expected `profiles` argument to be a non '
                             'empty sequence, got {}.'.format(profiles))

        if not all(isinstance(p, JobProfile) for p in profiles):
            raise TypeError('Expected `profiles` argument to be a '
                            'sequence of `JobProfile`, got {}'.format(profiles))

        super().__init__(name)
        self.__repeat = int(repeat)
        self.__profiles = list(profiles)

    @property
    def repeat(self) -> int:
        """The number of times that the profile sequence will repeat."""
        return self.__repeat

    @property
    def profiles(self) -> Sequence[JobProfile]:
        """The sequence of profiles to execute."""
        return self.__profiles


class ParallelHomogeneousPFSJobProfile(JobProfile):
    """ Batsim Homogeneous Job with IO to/from PFS Profile class.

    This class describes a job that represents an IO transfer between
    the allocated hosts and a storage resource. 

    Args:
        name: The profile name. Must be unique within a workload.
        bytes_to_read: The amount of bytes to read.
        bytes_to_write: The amount of bytes to write.
        storage: The storage resource label.

    Raises:
        ValueError: In case of both `bytes_to_read` and `bytes_to_write` arguments
            are not greater than zero or any of them is negative and `storage` 
            argument is not a valid string.

    Examples:
        >>> pfs = ParallelHomogeneousPFSJobProfile("pfs", bytes_to_read=10e6, bytes_to_write=1e6, storage="pfs")
    """

    def __init__(self,
                 name: str,
                 bytes_to_read: Union[int, float],
                 bytes_to_write: Union[int, float],
                 storage: str = 'pfs') -> None:
        if bytes_to_read == 0 and bytes_to_write == 0:
            raise ValueError('Expected `bytes_to_read` or `bytes_to_write` '
                             'arguments to be greater than zero.')
        if bytes_to_read < 0:
            raise ValueError('Expected `bytes_to_read` argument to be '
                             'not less than zero, got {}'.format(bytes_to_read))
        if bytes_to_write < 0:
            raise ValueError('Expected `bytes_to_write` argument to be '
                             'not less than zero, got {}'.format(bytes_to_write))
        if not storage:
            raise ValueError('Expected `storage` argument to be a '
                             'valid string, got {}'.format(storage))

        super().__init__(name)
        self.__bytes_to_read = float(bytes_to_read)
        self.__bytes_to_write = float(bytes_to_write)
        self.__storage = str(storage)

    @property
    def bytes_to_read(self) -> float:
        """ The amount of bytes to read. """
        return self.__bytes_to_read

    @property
    def bytes_to_write(self) -> float:
        """ The amount of bytes to write. """
        return self.__bytes_to_write

    @property
    def storage(self) -> str:
        """ The storage label """
        return self.__storage


class DataStagingJobProfile(JobProfile):
    """ Batsim Data Staging Job Profile class.

    This class describes a job that represents an IO transfer between
    two storage resources.

    Args:
        name: The profile name. Must be unique within a workload.
        nb_bytes: The amount of bytes to be transferred.
        src: The sending storage label (source).
        dest: The receiving storage label (destination).

    Raises:
        ValueError: In case of `nb_bytes` argument is not greater than or equal 
            to zero and `src` and `dest` arguments are not valid strings.

    Examples:
        >>> data = DataStagingJobProfile("data", nb_bytes=10e6, src="pfs", dest="nfs")
    """

    def __init__(self,
                 name: str,
                 nb_bytes: Union[int, float],
                 src: str,
                 dest: str) -> None:

        if nb_bytes <= 0:
            raise ValueError('Expected `nb_bytes` argument to be a '
                             'positive number, got {}'.format(nb_bytes))

        if not src:
            raise ValueError('Expected `src` argument to be a '
                             'valid string, got {}'.format(src))
        if not dest:
            raise ValueError('Expected `dest` argument to be a '
                             'valid string, got {}'.format(dest))

        super().__init__(name)
        self.__nb_bytes = float(nb_bytes)
        self.__src = str(src)
        self.__dest = str(dest)

    @property
    def dest(self) -> str:
        """ The receiving storage label."""
        return self.__dest

    @property
    def src(self) -> str:
        """ The sending storage label. """
        return self.__src

    @property
    def nb_bytes(self) -> float:
        """ The amount of bytes to be transferred. """
        return self.__nb_bytes


class Job(Identifier):
    """ Batsim Job class.

    This class describes a rigid job.

    Attributes:
        WORKLOAD_SEPARATOR: A char that separates the workload name from 
            the job name. By default Batsim submits the workload along with 
            the job name in the id field.

    Args:
        name: The job name. Must be unique within a workload.
        workload: The job workload name.
        res: The number of resources requested.
        profile: The job profile to be executed.
        subtime: The submission time.
        walltime: The execution time limit (maximum execution time).
        user: The job owner name.

    Raises:
        TypeError: In case of invalid arguments.
        ValueError: In case of invalid arguments value.
    """

    WORKLOAD_SEPARATOR: str = "!"

    def __init__(self,
                 name: str,
                 workload: str,
                 res: int,
                 profile: JobProfile,
                 subtime: Union[int, float],
                 walltime: Optional[Union[int, float]] = None,
                 user: Optional[str] = None) -> None:

        if not name:
            raise ValueError('Expected `name` argument to be a '
                             'valid string, got {}'.format(name))

        if not workload:
            raise ValueError('Expected `workload` argument to be a '
                             'valid string, got {}'.format(workload))

        if res <= 0:
            raise ValueError('Expected `res` argument to be greater '
                             'than zero, got {}'.format(res))

        if not isinstance(profile, JobProfile):
            raise TypeError('Expected `profile` argument to be a '
                            'instance of JobProfile, got {}'.format(profile))

        if subtime < 0:
            raise ValueError('Expected `subtime` argument to be greater '
                             'than or equal to zero, got {}'.format(subtime))

        if walltime is not None and walltime <= 0:
            raise ValueError('Expected `walltime` argument to be greater '
                             'than zero, got {}'.format(walltime))
                             

        job_id = "%s%s%s" % (str(workload), self.WORKLOAD_SEPARATOR, str(name))
        super().__init__(job_id)

        self.__res = int(res)
        self.__profile = profile
        self.__subtime = float(subtime)
        self.__walltime = walltime
        self.__user = user

        self.__state: JobState = JobState.UNKNOWN
        self.__allocation: Optional[Sequence[int]] = None
        self.__start_time: Optional[float] = None  # will be set on start
        self.__stop_time: Optional[float] = None  # will be set on terminate

    def __repr__(self) -> str:
        return "Job_%s" % self.id

    @property
    def name(self) -> str:
        """ The job name. """
        return self.id.split(self.WORKLOAD_SEPARATOR)[1]

    @property
    def workload(self) -> str:
        """ The job workload name. """
        return self.id.split(self.WORKLOAD_SEPARATOR)[0]

    @property
    def subtime(self) -> float:
        """ The job submission time. """
        return self.__subtime

    @property
    def res(self) -> int:
        """ The number of resources requested. """
        return self.__res

    @property
    def profile(self) -> JobProfile:
        """ The job profile. """
        return self.__profile

    @property
    def walltime(self) -> Optional[Union[int, float]]:
        """ The job maximum execution time. """
        return self.__walltime

    @property
    def user(self) -> Optional[str]:
        """ The job owner name. """
        return self.__user

    @property
    def state(self) -> JobState:
        """  The current job state. """
        return self.__state

    @property
    def allocation(self) -> Optional[Sequence[int]]:
        """ The allocated resources id. """
        return self.__allocation

    @property
    def is_running(self) -> bool:
        """ Whether this job is running or not. """
        return self.__state == JobState.RUNNING

    @property
    def is_runnable(self) -> bool:
        """ Whether this job is able to start. """
        return self.__state == JobState.ALLOCATED

    @property
    def is_submitted(self) -> bool:
        """ Whether this job was submitted. """
        return self.__state == JobState.SUBMITTED

    @property
    def is_finished(self) -> bool:
        """ Whether this job finished. """
        states = (JobState.COMPLETED_SUCCESSFULLY,
                  JobState.COMPLETED_FAILED,
                  JobState.COMPLETED_WALLTIME_REACHED,
                  JobState.COMPLETED_KILLED)

        return self.__state in states

    @property
    def is_rejected(self) -> bool:
        """ Whether this job was rejected. """
        return self.__state == JobState.REJECTED

    @property
    def start_time(self) -> Optional[float]:
        """ The job start time. """
        return self.__start_time

    @property
    def stop_time(self) -> Optional[float]:
        """ The job stop time. """
        return self.__stop_time

    @property
    def dependencies(self) -> Optional[Sequence[str]]:
        """ The id of the jobs it depends. """
        return None

    @property
    def waiting_time(self) -> Optional[float]:
        """ The job waiting time. """
        if self.start_time is None:
            return None
        else:
            return self.start_time - self.subtime

    @property
    def runtime(self) -> Optional[float]:
        """ The job runtime. """
        if self.stop_time is None or self.start_time is None:
            return None
        else:
            return self.stop_time - self.start_time

    @property
    def stretch(self) -> Optional[float]:
        """ The job stretch. """
        stretch = None
        if self.waiting_time is not None:
            if self.walltime is not None:
                stretch = self.waiting_time / self.walltime
            elif self.runtime is not None:
                stretch = self.waiting_time / self.runtime
        return stretch

    @property
    def turnaround_time(self) -> Optional[float]:
        """ The job turnaround time. """
        if self.waiting_time is None or self.runtime is None:
            return None
        else:
            return self.waiting_time + self.runtime

    @property
    def per_processor_slowdown(self) -> Optional[float]:
        """ The job per-processor slowdown. """
        if self.turnaround_time is None or self.runtime is None:
            return None
        else:
            return max(1., self.turnaround_time / (self.res * self.runtime))

    @property
    def slowdown(self) -> Optional[float]:
        """ The job slowdown. """
        if self.turnaround_time is None or self.runtime is None:
            return None
        else:
            return max(1., self.turnaround_time / self.runtime)

    def _allocate(self, hosts: Sequence[int]) -> None:
        """ Allocate hosts for the job. 

        This is an internal method to be used by the simulator only.

        Args:
            hosts: A sequence containing the allocated hosts ids.

        Raises:
            RuntimeError: In case of the job is already allocated or is not in
                the queue or the number of resources does not match the request.

        Dispatch:
            Event: JobEvent.ALLOCATED
        """
        if not self.is_submitted:
            raise RuntimeError('This job was already allocated or is not in '
                               'the queue, got {}'.format(self.state))
        if len(hosts) != self.res:
            raise RuntimeError('Expected `hosts` argument to be a list '
                               'of length {}, got {}'.format(self.res, hosts))

        self.__allocation = list(hosts)
        self.__state = JobState.ALLOCATED
        dispatcher.dispatch(JobEvent.ALLOCATED, self, unique=True)

    def _reject(self) -> None:
        """ Reject the job. 

        This is an internal method to be used by the simulator only.

        Raises:
            RuntimeError: In case of the job is not in the queue.

        Dispatch:
            Event: JobEvent.REJECTED
        """
        if not self.is_submitted:
            raise RuntimeError('Cannot reject a job that is not in the '
                               'queue, got {}'.format(self.state))

        self.__state = JobState.REJECTED
        dispatcher.dispatch(JobEvent.REJECTED, self, unique=True)

    def _submit(self, subtime: Union[int, float]) -> None:
        """ Submit the job. 

        This is an internal method to be used by the simulator only.

        Args:
            subtime: The submission time.

        Raises:
            RuntimeError: In case of the job was already submitted or 
                the subtime is less than zero.

        Dispatch:
            Event: JobEvent.SUBMITTED
        """
        if self.state != JobState.UNKNOWN:
            raise RuntimeError('This job was already submitted.'
                               'got, {}'.format(self.state))
        if subtime < 0:
            raise RuntimeError('Expected `subtime` argument to be greather '
                               'than zero, got {}'.format(subtime))

        self.__state = JobState.SUBMITTED
        self.__subtime = float(subtime)
        dispatcher.dispatch(JobEvent.SUBMITTED, self, unique=True)

    def _kill(self, current_time: Union[int, float]) -> None:
        """ Kill the job. 

        This is an internal method to be used by the simulator only.

        Args:
            current_time: The current simulation time.

        Raises:
            RuntimeError: In case of the job is not running or 
                the current time is less than the job start time.

        Dispatch:
            Event: JobEvent.COMPLETED
        """
        if not self.is_running or self.start_time is None:
            raise RuntimeError('The job cannot be killed if it is not running'
                               'got, {}'.format(self.state))

        if current_time < self.start_time:
            raise RuntimeError('Expected `current_time` argument to be greather '
                               'than start_time, got {}'.format(current_time))

        self.__stop_time = float(current_time)
        self.__state = JobState.COMPLETED_KILLED
        dispatcher.dispatch(JobEvent.COMPLETED, self, unique=True)

    def _start(self, current_time: Union[int, float]) -> None:
        """ Start the job. 

        This is an internal method to be used by the simulator only.

        Args:
            current_time: The current simulation time.

        Raises:
            RuntimeError: In case of the job is not runnable or the current 
                time is less than the submission time.

        Dispatch:
            Event: JobEvent.STARTED
        """
        if not self.is_runnable:
            raise RuntimeError('The job cannot start if it is not '
                               'runnable, got {}'.format(self.state))

        if current_time < self.subtime:
            raise RuntimeError('The `current_time` argument cannot be less '
                               'than the job submission time, '
                               'got {}'.format(current_time))

        self.__start_time = float(current_time)
        self.__state = JobState.RUNNING
        dispatcher.dispatch(JobEvent.STARTED, self, unique=True)

    def _terminate(self, current_time: Union[int, float], state: JobState) -> None:
        """ Terminate the job. 

        This is an internal method to be used by the simulator only.

        Args:
            current_time: The current simulation time.
            state: The last state of the job.

        Raises:
            RuntimeError: In case of the job is not running or 
                the current time is less than the job start time or 
                the state is not one of the possible ones.

        Dispatch:
            Event: JobEvent.COMPLETED

        """
        if not self.is_running or self.start_time is None:
            raise RuntimeError('The job cannot be finished if it is not running'
                               'got, {}'.format(self.state))

        if not state in (JobState.COMPLETED_SUCCESSFULLY, JobState.COMPLETED_FAILED, JobState.COMPLETED_WALLTIME_REACHED):
            raise RuntimeError('Expected `state` argument to be one of '
                               '[SUCCESSFULLY, FAILED, WALLTIME_REACHED], '
                               'got {}'.format(state))

        if current_time < self.start_time:
            raise RuntimeError('Expected `current_time` argument to be greather '
                               'than start_time, got {}'.format(current_time))

        self.__stop_time = float(current_time)
        self.__state = state
        dispatcher.dispatch(JobEvent.COMPLETED, self, unique=True)
