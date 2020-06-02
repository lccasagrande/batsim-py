from abc import ABC
from enum import Enum
from typing import Optional
from typing import Sequence
from typing import Dict


class JobState(Enum):
    """ Batsim Job State Types """

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
        delay: The sleep time.

    Raises:
        ValueError: In case of delay is not greater than zero.

    Examples:
        A job that will sleep for 100 seconds.

        >>> profile = DelayJobProfile(name="delay", delay=100)
    """

    def __init__(self, name: str, delay: float) -> None:
        if delay <= 0:
            raise ValueError('Expected `delay` argument to be a number '
                             'greater than zero, got {}'.format(delay))

        super().__init__(name)
        self.__delay = float(delay)

    @property
    def delay(self) -> float:
        """The sleep time. """
        return self.__delay


class ParallelJobProfile(JobProfile):
    """ Batsim Parallel Job Profile class.

    This class describes a job that performs a set of computations and 
    communications on the allocated hosts.

    Args:
        name: The profile name. Must be unique within a workload.
        cpu: A list that defines the amount of flops to be computed on each 
            allocated host.
        com: A list [host x host] that defines the amount of bytes to be 
            transferred between the allocated hosts.

    Raises:
        ValueError: In case of `cpu` is not provided or `com` has an invalid size 
            or their values are less than zero.

    Examples:
        Two hosts computing 10E6 flops each with local communication only.

        >>> profile = ParallelJobProfile(name="parallel", cpu=[10e6, 10e6], com=[5e6, 0, 0, 5e6])

        Two hosts computing 2E6 flops each with host 1 sending 5E6 bytes to host 2.

        >>> profile = ParallelJobProfile(name="parallel", cpu=[2e6, 2e6], com=[0, 5e6, 0, 0])

        One host computing 2E6 flops with host 1 sending 5E6 bytes to host 2 and host 2 sending 10E6 bytes to host 1.

        >>> profile = ParallelJobProfile(name="parallel", cpu=[2e6, 0], com=[0, 5e6, 10e6, 0])
    """

    def __init__(self,
                 name: str,
                 cpu: Sequence[float],
                 com: Sequence[float]) -> None:
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
    def cpu(self) -> Sequence[float]:
        """The amount of flops to be computed on each host."""
        return self.__cpu

    @property
    def com(self) -> Sequence[float]:
        """The amount of bytes to be transferred between hosts."""
        return self.__com


class ParallelHomogeneousJobProfile(JobProfile):
    """ Batsim Parallel Homogeneous Job Profile class.

    This class describes a job that performs the same computation and 
    communication on all allocated hosts.

    Args:
        name: The profile name. Must be unique within a workload.
        cpu: The amount of flops to be computed on each allocated host.
        com: The amount of bytes to be sent and received between each pair of hosts. 
            The loopback communication of each machine defaults to 0.

    Raises:
        ValueError: In case of `com` or `cpu` are not greater than zero.

    Examples:
        >>> profile = ParallelHomogeneousJobProfile("name", cpu=10e6, com=5e6)
    """

    def __init__(self,
                 name: str,
                 cpu: float,
                 com: float) -> None:
        if cpu == 0 and com == 0:
            raise ValueError('Expected `cpu` or `com` argument '
                             ' to be defined.')
        if cpu < 0:
            raise ValueError('Expected `cpu` argument to be '
                             'not less than zero.')
        if com < 0:
            raise ValueError('Expected `com` argument to be '
                             'not less than zero.')

        super().__init__(name)
        self.__cpu = float(cpu)
        self.__com = float(com)

    @property
    def cpu(self) -> float:
        """The amount of flops to be computed on each host."""
        return self.__cpu

    @property
    def com(self) -> float:
        """The amount of bytes to be transferred between hosts."""
        return self.__com


class ParallelHomogeneousTotalJobProfile(JobProfile):
    """ Batsim Parallel Homogeneous Total Job Profile class.

    This class describes a job that equally distributes the total amount of 
    computation and communication between the allocated hosts.

    Args:
        name: The profile name. Must be unique within a workload.
        cpu: The total amount of flops to be computed on all hosts.
        com: The total amount of bytes to be sent and received on each
            pair of hosts.

    Raises:
        ValueError: In case of `com` or `cpu` are not greater than zero.

    Examples:
        >>> profile = ParallelHomogeneousTotalJobProfile("name", cpu=10e6, com=5e6)
    """

    def __init__(self,
                 name: str,
                 cpu:  float,
                 com:  float) -> None:
        if cpu == 0 and com == 0:
            raise ValueError('Expected `cpu` or `com` arguments '
                             ' to be defined.')
        if cpu < 0:
            raise ValueError('Expected `cpu` argument to be '
                             'not less than zero.')
        if com < 0:
            raise ValueError('Expected `com` argument to be '
                             'not less than zero.')

        super().__init__(name)
        self.__cpu = float(cpu)
        self.__com = float(com)

    @property
    def cpu(self) -> float:
        """The total amount of flops to be computed."""
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
        profiles: The profile names to execute sequentially.
        repeat: The number of times to repeat the sequence. Defaults to 1.

    Raises:
        ValueError: In case of `repeat` argument is less than 1 or profiles list 
            is empty.

    Examples:
        >>> composed = ComposedJobProfile("composed", profiles=["p1", "p2"], repeat=2)
    """

    def __init__(self,
                 name: str,
                 profiles: Sequence[str],
                 repeat: int = 1) -> None:
        if repeat <= 0:
            raise ValueError('Expected `repeat` argument to be greater'
                             'than 0, got {}'.format(repeat))

        if not profiles:
            raise ValueError('Expected `profiles` argument to be a non '
                             'empty sequence, got {}.'.format(profiles))

        super().__init__(name)
        self.__repeat = int(repeat)
        self.__profiles = list(profiles)

    @property
    def repeat(self) -> int:
        """The number of times to repeat the profile sequence."""
        return self.__repeat

    @property
    def profiles(self) -> Sequence[str]:
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
        ValueError: In case of `bytes_to_read` or `bytes_to_write` are not greater 
            than zero or `storage` is not a valid string.

    Examples:
        >>> pfs = ParallelHomogeneousPFSJobProfile("pfs", bytes_to_read=10e6, bytes_to_write=1e6, storage="pfs")
    """

    def __init__(self,
                 name: str,
                 bytes_to_read: float,
                 bytes_to_write: float,
                 storage: str) -> None:
        if bytes_to_read == 0 and bytes_to_write == 0:
            raise ValueError('Expected `bytes_to_read` or `bytes_to_write` '
                             'arguments to be defined.')
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
        """ The storage label. """
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
            to zero and `src` or `dest` arguments are not valid strings.

    Examples:
        >>> data = DataStagingJobProfile("data", nb_bytes=10e6, src="pfs", dest="nfs")
    """

    def __init__(self,
                 name: str,
                 nb_bytes: float,
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


class Job:
    """ This class describes a rigid job.

    Args:
        name: The job name. Must be unique within a workload.
        workload: The job workload name.
        res: The number of resources requested.
        profile: The job profile to be executed.
        subtime: The submission time.
        walltime: The execution time limit (maximum execution time). 
            Defaults to None.
        user_id: The user id. Defaults to None.

    Raises:
        ValueError: In case of invalid arguments value.
    """

    WORKLOAD_SEPARATOR: str = "!"

    def __init__(self,
                 name: str,
                 workload: str,
                 res: int,
                 profile: JobProfile,
                 subtime: float,
                 walltime: Optional[float] = None,
                 user_id: Optional[int] = None) -> None:

        if not name:
            raise ValueError('Expected `name` argument to be a '
                             'valid string, got {}'.format(name))

        if not workload:
            raise ValueError('Expected `workload` argument to be a '
                             'valid string, got {}'.format(workload))

        if res <= 0:
            raise ValueError('Expected `res` argument to be greater '
                             'than zero, got {}'.format(res))

        if subtime < 0:
            raise ValueError('Expected `subtime` argument to be greater '
                             'than or equal to zero, got {}'.format(subtime))

        if walltime is not None and walltime <= 0:
            raise ValueError('Expected `walltime` argument to be greater '
                             'than zero, got {}'.format(walltime))

        assert isinstance(profile, JobProfile)
        self.__id = f"{workload}{self.WORKLOAD_SEPARATOR}{name}"
        self.__res = int(res)
        self.__profile = profile
        self.__subtime = float(subtime)
        self.__walltime = walltime
        self.__user_id = user_id
        self.__storage_mapping: Optional[Dict[str, int]] = None
        self.metadata: dict = {}

        self.__state: JobState = JobState.UNKNOWN
        self.__allocation: Optional[Sequence[int]] = None
        self.__start_time: Optional[float] = None  # will be set on start
        self.__stop_time: Optional[float] = None  # will be set on terminate

    def __repr__(self) -> str:
        return f"Job_{self.id}"

    @property
    def id(self) -> str:
        """ The job ID. """
        return self.__id

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
    def storage_mapping(self) -> Optional[Dict[str, int]]:
        """ The job storage mapping. """
        return self.__storage_mapping

    @property
    def res(self) -> int:
        """ The number of resources requested. """
        return self.__res

    @property
    def profile(self) -> JobProfile:
        """ The job profile. """
        return self.__profile

    @property
    def walltime(self) -> Optional[float]:
        """ The job maximum execution time. """
        return self.__walltime

    @property
    def user_id(self) -> Optional[int]:
        """ The user id. """
        return self.__user_id

    @property
    def state(self) -> JobState:
        """  The current job state. """
        return self.__state

    @property
    def allocation(self) -> Optional[Sequence[int]]:
        """ The allocated resource ids. """
        if self.__allocation:
            return list(self.__allocation)
        else:
            return None

    @property
    def is_running(self) -> bool:
        """ Whether this job is running or not. """
        return self.__state == JobState.RUNNING

    @property
    def is_runnable(self) -> bool:
        """ Whether this job is able to start or not. """
        return self.__state == JobState.ALLOCATED

    @property
    def is_submitted(self) -> bool:
        """ Whether this job was submitted or not. """
        return self.__state == JobState.SUBMITTED

    @property
    def is_finished(self) -> bool:
        """ Whether this job has finished or not. """
        states = (JobState.COMPLETED_SUCCESSFULLY,
                  JobState.COMPLETED_FAILED,
                  JobState.COMPLETED_WALLTIME_REACHED,
                  JobState.COMPLETED_KILLED)

        return self.__state in states

    @property
    def is_rejected(self) -> bool:
        """ Whether this job was rejected or not. """
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
    def waiting_time(self) -> Optional[float]:
        """ The job waiting time. """
        if self.start_time is None:
            return None
        else:
            return self.start_time - self.subtime

    @property
    def runtime(self) -> Optional[float]:
        """ The job runtime. 

        The minimum value is 1 second.
        """
        if self.stop_time is None or self.start_time is None:
            return None
        else:
            return max(1, self.stop_time - self.start_time)

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

    def _allocate(self, hosts: Sequence[int], storage_mapping: Dict[str, int] = None) -> None:
        """ Allocate hosts for the job. 

        This is an internal method to be used by the simulator only.

        Args:
            hosts: A sequence of host ids.
            storage_mapping: The resource ids of the storages to use. 
                Defaults to None. Required only if the job needs a storage 
                resource.

        Raises:
            RuntimeError: In case of job is not in the queue (e.g. job is 
                runnable or running).
            ValueError: In case of the number of allocated resources does not 
                match the number requested or the job needs a storage and 
                the mapping is not defined or valid.
        """
        if not self.is_submitted:
            raise RuntimeError('The job is not in the queue anymore, '
                               f'got {self.state}')
        if len(hosts) != self.res:
            raise ValueError('Expected `hosts` argument to be a list '
                             f'of length {self.res}, got {hosts}')

        if isinstance(self.profile, DataStagingJobProfile):
            if not storage_mapping:
                raise ValueError(f'Expected storage mapping to be defined, '
                                 f'got {storage_mapping}')

            if self.profile.src not in storage_mapping:
                raise ValueError(f'Expected storage mapping to '
                                 f'include {self.profile.src}')

            if self.profile.dest not in storage_mapping:
                raise ValueError(f'Expected storage mapping to '
                                 f'include {self.profile.dest}')
        elif isinstance(self.profile, ParallelHomogeneousPFSJobProfile):
            if not storage_mapping:
                raise ValueError(f'Expected storage mapping to be defined, '
                                 f'got {storage_mapping}')
            if self.profile.storage not in storage_mapping:
                raise ValueError(f'Expected storage mapping to '
                                 f'include {self.profile.storage}')
        elif storage_mapping:
            raise ValueError(f'Expected storage mapping to be empty, '
                             f'got {storage_mapping}')

        self.__allocation = list(hosts)
        self.__storage_mapping = storage_mapping
        self.__state = JobState.ALLOCATED

    def _reject(self) -> None:
        """ Rejects the job. 

        This is an internal method to be used by the simulator only.

        Raises:
            RuntimeError: In case of job is not in the queue (e.g. job 
                is runnable or running).
        """
        if not self.is_submitted:
            raise RuntimeError('Cannot reject a job that is not in the '
                               f'queue, got {self.state}')

        self.__state = JobState.REJECTED

    def _submit(self, subtime: float) -> None:
        """ Submits the job. 

        This is an internal method to be used by the simulator only.

        Args:
            subtime: The submission time.

        Raises:
            RuntimeError: In case of job was already submitted.
            ValueError: In case of subtime is invalid.
        """
        if self.state != JobState.UNKNOWN:
            raise RuntimeError('This job was already submitted.'
                               f'got, {self.state}')
        if subtime < 0:
            raise ValueError('Expected `subtime` argument to be greather '
                             f'than zero, got {subtime}')

        self.__state = JobState.SUBMITTED
        self.__subtime = float(subtime)

    def _start(self, current_time: float) -> None:
        """ Start the job. 

        This is an internal method to be used by the simulator only.

        Args:
            current_time: The current simulation time.

        Raises:
            RuntimeError: In case of job is not runnable
            ValueError: In case of current time is less than the submission time.
        """
        if not self.is_runnable:
            raise RuntimeError('The job cannot start if it is not '
                               f'runnable, got {self.state}')

        if current_time < self.subtime:
            raise ValueError('The `current_time` argument cannot be less '
                             'than the job submission time, '
                             f'got {current_time}')

        self.__start_time = float(current_time)
        self.__state = JobState.RUNNING

    def _terminate(self, current_time: float, final_state: JobState) -> None:
        """ Terminate the job. 

        This is an internal method to be used by the simulator only.

        Args:
            current_time: The current simulation time.
            final_state: The job final state.

        Raises:
            RuntimeError: In case of the job is not running.
            ValueError: In case of current time is less than the job start 
                time or the final state is not one of the possible ones.
        """
        if not self.is_running or self.start_time is None:
            raise RuntimeError('The job cannot be finished if it is not running'
                               f'got, {self.state}')

        valid_states = (JobState.COMPLETED_SUCCESSFULLY,
                        JobState.COMPLETED_FAILED,
                        JobState.COMPLETED_KILLED,
                        JobState.COMPLETED_WALLTIME_REACHED)

        if not final_state in valid_states:
            raise ValueError('Expected `final_state` argument to be valid, '
                             f'got {final_state}')

        if current_time < self.start_time:
            raise ValueError('Expected `current_time` argument to be greather '
                             f'than start_time, got {current_time}')

        self.__stop_time = float(current_time)
        self.__state = final_state
