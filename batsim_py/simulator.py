import atexit
from collections import defaultdict
import decimal
from shutil import which
import signal
import subprocess
import sys
import tempfile
from typing import Dict
from typing import Callable, Union
from typing import Sequence
from typing import List
from typing import DefaultDict
from typing import Optional
from typing import Iterator
from typing import NamedTuple
from typing import overload
from typing import Literal

import numpy as np

from .events import SimulatorEvent
from .events import JobEvent
from .events import HostEvent
from .jobs import Job
from .protocol import NotifyBatsimEvent
from .protocol import BatsimNotifyType
from .protocol import SimulationBeginsBatsimEvent
from .protocol import NetworkHandler
from .protocol import BatsimMessage
from .protocol import ResourcePowerStateChangedBatsimEvent
from .protocol import BatsimEventType
from .protocol import JobCompletedBatsimEvent
from .protocol import JobSubmittedBatsimEvent
from .protocol import BatsimRequest
from .protocol import CallMeLaterBatsimRequest
from .protocol import KillJobBatsimRequest
from .protocol import ExecuteJobBatsimRequest
from .protocol import RejectJobBatsimRequest
from .protocol import SetResourceStateBatsimRequest
from .resources import Platform
from .resources import Host
from .utils import get_free_tcp_address


# Type alias
EventSenders = Union[Host, Job, 'SimulatorHandler']
JobListener = Callable[[Job], None]
HostListener = Callable[[Host], None]
SimulatorListener = Callable[['SimulatorHandler'], None]
Listener = Union[JobListener, HostListener, SimulatorListener]
Event = Union[JobEvent, HostEvent, SimulatorEvent]
Listeners = DefaultDict[Event, List[Listener]]
Callback = Callable[[float], None]
Callbacks = DefaultDict[float, List[Callback]]
BatsimVerbosity = Literal["quiet", "network-only", "information", "debug"]


class Reservation:
    """ Describes the reservation of a host.

    Args:
        host: The host.
        release_time: The remaining time to the host be released.

    Attributes:
        host: The host.
        release_time: The remaining time to the host be released.
    """

    def __init__(self, host: Host, release_time: float) -> None:
        self.host = host
        self.release_time = release_time


class SimulatorHandler:
    """ Simulator handler class.

    This class will handle the Batsim simulation process.

    Args:
        tcp_address: An address string consisting of three parts as follows:
            protocol://interface:port. Defaults to None. If no address
            is provided, a random one will be used.

    Raises:
        ImportError: In case of Batsim cannot be found.

    Examples:
        >>> handler = SimulatorHandler("tcp://localhost:28000")
    """

    def __init__(self, tcp_address: Optional[str] = None) -> None:
        if which('batsim') is None:
            raise ImportError('(HINT: you need to install Batsim. '
                              'Check the setup instructions here: '
                              'https://batsim.readthedocs.io/en/latest/.). '
                              'Run "batsim --version" to make sure it is working.')
        self.__network = NetworkHandler(tcp_address or get_free_tcp_address())
        self.__current_time: float = 0.
        self.__simulator: Optional[subprocess.Popen] = None
        self.__simulation_time: Optional[float] = None
        self.__platform: Platform = None  # type: ignore
        self.__no_more_jobs_to_submit = False
        self.__no_more_external_event_to_occur = False
        self.__batsim_requests: List[BatsimRequest] = []
        self.__jobs: List[Job] = []
        self.__callbacks: Callbacks = defaultdict(list)
        self.__subscriptions: Listeners = defaultdict(list)

        # Batsim events handlers
        self.__batsim_event_handlers: dict = {
            BatsimEventType.SIMULATION_ENDS: self.__on_batsim_simulation_ends,
            BatsimEventType.SIMULATION_BEGINS: self.__on_batsim_simulation_begins,
            BatsimEventType.JOB_COMPLETED: self.__on_batsim_job_completed,
            BatsimEventType.JOB_SUBMITTED: self.__on_batsim_job_submitted,
            BatsimEventType.RESOURCE_STATE_CHANGED: self.__on_batsim_host_state_changed,
            BatsimEventType.REQUESTED_CALL: self.__on_batsim_requested_call,
            BatsimEventType.NOTIFY: self.__on_batsim_notify
        }

        atexit.register(self.__close_simulator)
        signal.signal(signal.SIGTERM, self.__on_sigterm)

    @property
    def address(self) -> str:
        """ The address string. """
        return self.__network.address

    @property
    def jobs(self) -> Sequence[Job]:
        """ A sequence with all the jobs in the system.

        This does not include jobs that already finished.
        """
        return list(self.__jobs)

    @property
    def queue(self) -> Sequence[Job]:
        """ A sequence of all jobs in the queue. """
        return [j for j in self.__jobs if j.is_submitted]

    @property
    def agenda(self) -> Iterator[Reservation]:
        """ Host reservations. """
        if self.__platform:
            for host in self.__platform.hosts:
                release_t = 0.
                for job_id in host.jobs:
                    job = next(j for j in self.__jobs if j.id == job_id)
                    if job.walltime:
                        runtime = 0.
                        if job.is_running:
                            assert job.start_time is not None
                            runtime = self.current_time - job.start_time
                        job_release_t = job.walltime - runtime
                    else:
                        job_release_t = np.inf

                    release_t = max(release_t, job_release_t)
                yield Reservation(host, release_t)

    @property
    def platform(self) -> Platform:
        """ The simulation platform."""
        return self.__platform

    @property
    def is_running(self) -> bool:
        """ Whether the simulation is running or not."""
        return self.__simulator is not None

    @property
    def current_time(self) -> float:
        """ The current simulation time. """
        t = self.__current_time

        # Truncate:
        t = float(decimal.Decimal(t).quantize(decimal.Decimal('1.'),
                                              rounding=decimal.ROUND_DOWN))
        return t

    @property
    def is_submitter_finished(self) -> bool:
        """ Whether there are still some jobs to be submitted or not. """
        return self.__no_more_jobs_to_submit

    def start(self,
              platform: str,
              workload: str,
              verbosity: BatsimVerbosity = "quiet",
              simulation_time: Optional[float] = None,
              allow_compute_sharing=False,
              allow_storage_sharing=True,
              external_events: Optional[str] = None) -> None:
        """ Start the simulation process.

        Args:
            platform: The XML file defining the platform. It must follow the
                format expected by Batsim and SimGrid. Check their documentation 
                to know how to define a platform
            workload: A JSON file defining the jobs and their profiles.
                The simulation process will only submit the jobs that are
                defined in this JSON. Moreover, Batsim is responsible for 
                the submission process.
            verbosity: The Batsim verbosity level. Defaults to "quiet".
            simulation_time: The maximum simulation time. Defaults to None.
                If this argument is set, the simulation will stop only when this
                time is reached, no matter if there are jobs to be submitted or
                running. Otherwise, the simulation will only stop when all jobs
                in the workload were completed or rejected.
            allow_compute_sharing: Whether a host can be used by multiple jobs 
                or not. Defaults to False.
            allow_storage_sharing: Whether a storage can be used by multiple jobs 
                or not. Defaults to True.
            external_events: The file containing external events to simulate. 
                Check the Batsim documentation to know what kind of external 
                events are supported.

        Raises:
            RuntimeError: In case of simulation already running.
            ValueError: In case of `simulation_time` is less than or equals to
                zero or the verbosity is invalid.

        Examples:
            >>> handler = SimulatorHandler("tcp://localhost:28000")
            >>> handler.start("platform.xml", "workload.json", "information", 1440)

        """
        assert workload and platform

        if self.is_running:
            raise RuntimeError("The simulation is already running.")

        if verbosity not in ("quiet", "network-only", "information", "debug"):
            raise ValueError('This `verbosity` argument value is not accepted '
                             f'by Batsim, got {verbosity}')

        if simulation_time is not None and simulation_time <= 0:
            raise ValueError('Expected `simulation_time` to be greater '
                             f'than zero, got {simulation_time}.')

        self.__jobs = []
        self.__current_time = 0.
        self.__simulation_time = simulation_time
        self.__no_more_jobs_to_submit = False

        # There isn't an option to avoid exporting batsim results
        tmp_dir = tempfile.gettempdir() + "/batsim"
        cmd = (
            'batsim -E --forward-profiles-on-submission '
            '--disable-schedule-tracing --disable-machine-state-tracing '
            f'-s {self.__network.address} -p {platform} -w {workload} '
            f'-v {verbosity}  -e {tmp_dir}'
        )

        if allow_compute_sharing:
            cmd += ' --enable-compute-sharing'
        if not allow_storage_sharing:
            cmd += ' --disable-storage-sharing'

        if external_events:
            self.__no_more_external_event_to_occur = False
            cmd += f' --events {external_events}'
        else:
            self.__no_more_external_event_to_occur = True

        self.__simulator = subprocess.Popen(
            cmd.split(), stdout=subprocess.PIPE)

        self.__network.bind()

        self.__handle_batsim_events()
        if self.__simulation_time:
            self.__set_batsim_call_me_later(self.__simulation_time)

        self.__dispatch_event(SimulatorEvent.SIMULATION_BEGINS, self)

    def close(self) -> None:
        """ Close the simulation process. """
        if self.is_running:
            self.__close_simulator()
            self.__network.close()
            self.__simulation_time = None
            self.__batsim_requests.clear()
            self.__callbacks.clear()
            self.__dispatch_event(SimulatorEvent.SIMULATION_ENDS, self)

    def proceed_time(self, time: int = 0) -> None:
        """ Proceed the simulation process to the next event or time.

        Args:
            time: The time to proceed. Defaults to 0. It's possible to proceed
                directly to the next event or to a specific time. If the time is
                unset (equals 0), the simulation will proceed to the next 
                event. Otherwise, if a time 't' is provided, the simulation will 
                proceed directly to it and only events that occur before 't' will 
                be dispatched. 

        Raises:
            ValueError: In case of invalid arguments value.
            RuntimeError: In case of the simulation is not running or
                a deadlock happened. The latter case occurs only when there are 
                no more events to happen. Consequently, the simulation does not 
                know what to do and a deadlock error is raised by Batsim.
        """

        def unflag(_):
            # this a internal function to be called by the callback procedure.
            self.__wait_callback = False

        if not self.is_running:
            raise RuntimeError("The simulation is not running.")

        if time < 0:
            raise ValueError('Expected `time` argument to be a number '
                             f'greater than zero, got {time}.')

        if not time:
            # Go to the next event.
            self.__wait_callback = False
        elif not self.__simulation_time and self.is_submitter_finished and not self.__jobs and self.__no_more_external_event_to_occur:
            # There are no more actions to do. Go to the next event.
            self.__wait_callback = False
        else:
            # Setup a call me later request.
            self.__wait_callback = True
            self.set_callback(time + self.current_time, unflag)

        self.__goto_next_batsim_event()
        self.__start_runnable_jobs()
        while self.is_running and self.__wait_callback:
            self.__goto_next_batsim_event()
            self.__start_runnable_jobs()

    @overload
    def subscribe(self, event: JobEvent, call: JobListener) -> None: ...

    @overload
    def subscribe(self, event: HostEvent, call: HostListener) -> None: ...

    @overload
    def subscribe(self, event: SimulatorEvent,
                  call: SimulatorListener) -> None: ...

    def subscribe(self, event: Event, call: Listener) -> None:
        """ Subscribe to an event.

        Args:
            event: The event to subscribe.
            call: The function to be called when the event is dispatched. It
                must accept the event sender as an argument.

        Raises:
            RuntimeError: In case of simulation is not running.
        """
        assert callable(call)
        self.__subscriptions[event].append(call)

    def set_callback(self, at: float, call: Callable[[float], None]) -> None:
        """ Setup a callback.

        The simulation will call the function at the defined time.

        Args:
            at: The time which the function must be called.
            call: A function that receives the current simulation time as 
                an argument.

        Raises:
            ValueError: In case of invalid arguments value.
            RuntimeError: In case of simulation is not running.
        """
        assert callable(call)

        if not self.is_running:
            raise RuntimeError("The simulation is not running.")

        if at <= self.current_time:
            raise ValueError('Expected `at` argument to be a number '
                             'greater than the current simulation time'
                             f', got {at}.')

        self.__callbacks[at].append(call)
        self.__set_batsim_call_me_later(at)

    def allocate(self,
                 job_id: str,
                 hosts_id: Sequence[int],
                 storage_mapping: Dict[str, int] = None) -> None:
        """ Allocate resources for a job.

        To start computing, a job must allocate some resources first. When these 
        resources are ready, the simulator will automatically start the job. If 
        for some reason any allocated resource is not ready (because it’s off or 
        switching On/Off), the simulator will try to initialize the resource and 
        will wait until it’s ready to start the job.

        Args:
            job_id: The job id.
            hosts_id: A sequence of host ids to be allocated for the job.
            storage_mapping: A mapping of storage names to resource ids. If 
                the job needs a storage resource, this argument is required. 
                Otherwise, it can just be ignored.

        Raises:
            RuntimeError: In case of simulation is not running.
            LookupError: In case of job/resource not found or resource type 
                is invalid.
        """
        assert job_id and hosts_id

        if not self.is_running:
            raise RuntimeError("The simulation is not running.")

        assert self.__platform, "For some reason the platform was not loaded"

        job = next((j for j in self.__jobs if j.id == job_id), None)
        if not job:
            raise LookupError("The job {} was not found.".format(job_id))

        # Allocate hosts
        for h_id in hosts_id:
            self.__platform.get_host(h_id)._allocate(job.id)

        # Allocate storages
        if storage_mapping:
            for h_id in set(storage_mapping.values()):
                self.__platform.get_storage(h_id)._allocate(job.id)

        # Allocate job
        job._allocate(hosts_id, storage_mapping)
        self.__dispatch_event(JobEvent.ALLOCATED, job)

        # Start job
        self.__start_runnable_jobs()

    def kill_job(self, job_id: str) -> None:
        """ Kill a job that is running.

        Args:
            job_id: The id of the job to kill.

        Raises:
            RuntimeError: In case of simulation is not running or the job 
                cannot be killed.
            LookupError: In case of job not found.
        """
        if not self.is_running:
            raise RuntimeError("The simulation is not running.")

        job = next((j for j in self.__jobs if j.id == job_id), None)
        if not job:
            raise LookupError("The job {} was not found.".format(job_id))

        if not job.is_running:
            raise RuntimeError(f"Invalid kill. Job {job.id} is not running")

        # Sync now with Batsim
        request = KillJobBatsimRequest(self.current_time, job.id)
        msg = BatsimMessage(self.current_time, [request])
        self.__network.send(msg)
        self.__handle_batsim_events()

    def reject_job(self, job_id: str) -> None:
        """ Rejects a job.

        Only jobs in the queue can be rejected. Once a job is rejected, it 
        cannot be scheduled anymore.

        Args:
            job_id: The id of the job to reject.

        Raises:
            RuntimeError: In case of simulation is not running or the job 
                cannot be rejected.
            LookupError: In case of job not found.
        """

        if not self.is_running:
            raise RuntimeError("The simulation is not running.")

        job = next((j for j in self.__jobs if j.id == job_id), None)
        if not job:
            raise LookupError("The job {} was not found.".format(job_id))

        job._reject()
        self.__jobs.remove(job)

        # Sync Batsim
        request = RejectJobBatsimRequest(self.current_time, job_id)
        self.__batsim_requests.append(request)
        self.__dispatch_event(JobEvent.REJECTED, job)

    def switch_on(self, hosts_id: Sequence[int]) -> None:
        """ Switch on hosts.

        Args:
            hosts_id: A sequence of host ids to be switched on.

        Raises:
            RuntimeError: In case of the simulation is not running or the host 
                cannot switch on because it's in an invalid state or the power 
                state is not defined.
            LookupError: In case of host not found.
        """
        if not self.is_running:
            raise RuntimeError("The simulation is not running.")

        assert self.__platform, "For some reason, the platform was not loaded."

        for h_id in hosts_id:
            host = self.__platform.get_host(h_id)
            host._switch_on()
            ending_pstate = host.get_default_pstate()

            # Sync Batsim
            self.__set_batsim_host_pstate(host.id, ending_pstate.id)
            self.__dispatch_event(HostEvent.STATE_CHANGED, host)

    def switch_off(self, hosts_id: Sequence[int]) -> None:
        """ Switch off hosts.

        Args:
            hosts_id: A sequence of host ids to be switched off.

        Raises:
            RuntimeError: In case of simulation is not running or a host cannot 
                switch off because it's in an invalid state or the power state is 
                not defined.
            LookupError: In case of host not found or the 'off' power state could 
                not be found.
        """
        if not self.is_running:
            raise RuntimeError("The simulation is not running.")

        assert self.__platform, "For some reason, the platform was not loaded."

        for h_id in hosts_id:
            host = self.__platform.get_host(h_id)
            host._switch_off()
            ending_pstate = host.get_sleep_pstate()

            # Sync Batsim
            self.__set_batsim_host_pstate(host.id, ending_pstate.id)
            self.__dispatch_event(HostEvent.STATE_CHANGED, host)

    def switch_power_state(self, host_id: int, pstate_id: int) -> None:
        """ Switch the host computation power state.

        This is useful if you want to implement a DVFS policy.

        Args:
            host_id: The host id.
            pstate_id: The computation power state id.

        Raises:
            RuntimeError: In case of the simulation is not running or the 
                power state were is defined or the host cannot switch.
            LookupError: In case of host or power state could not be found.
        """
        if not self.is_running:
            raise RuntimeError("The simulation is not running.")

        assert self.__platform, "For some reason, the platform was not loaded."

        host = self.__platform.get_host(host_id)
        host._set_computation_pstate(pstate_id)

        # Sync Batsim
        assert host.pstate
        self.__set_batsim_host_pstate(host_id, pstate_id)
        self.__dispatch_event(HostEvent.COMPUTATION_POWER_STATE_CHANGED,
                              host)

    def __dispatch_event(self, event: Event, sender: EventSenders) -> None:
        """ Dispatch an simulator event """
        if isinstance(event, JobEvent):
            assert isinstance(sender, Job), "JobEvent sender must be a Job"
        elif isinstance(event, HostEvent):
            assert isinstance(sender, Host), "HostEvent sender must be a Host"
        else:
            msg = 'SimulatorEvent sender must be a SimulatorHandler'
            assert isinstance(sender, SimulatorHandler), msg

        for call in self.__subscriptions[event]:
            call(sender)  # type: ignore

    def __start_runnable_jobs(self) -> None:
        """ Start runnable jobs.

        This is an internal method used to starts jobs that were allocated. 
        A job can only starts if the hosts are idle. Thus, this method ensures 
        that the host can compute the job.
        """
        if not self.is_running:
            return

        assert self.__platform, "For some reason, the platform was not loaded."

        runnable_jobs = [j for j in self.__jobs if j.is_runnable]
        for job in runnable_jobs:
            assert job.allocation, "For some reason, the job was not allocated."

            is_ready = True
            hosts = [self.__platform.get_host(h) for h in job.allocation]

            # Check if all hosts are active and switch on sleeping hosts
            for host in hosts:
                if not host.is_idle and not host.is_computing:
                    is_ready = False
                if host.is_sleeping:
                    self.switch_on([host.id])

            if is_ready:
                job._start(self.current_time)
                for host in hosts:
                    if not host.is_computing:
                        host._start_computing()
                        self.__dispatch_event(HostEvent.STATE_CHANGED, host)

                self.__dispatch_event(JobEvent.STARTED, job)
                # Sync Batsim
                request = ExecuteJobBatsimRequest(self.current_time,
                                                  job.id,
                                                  job.allocation,
                                                  job.storage_mapping)
                self.__batsim_requests.append(request)

    def __goto_next_batsim_event(self) -> None:
        """ Go to the next Batsim event. """
        self.__send_requests()
        self.__handle_batsim_events()
        if self.__simulation_time and self.current_time >= self.__simulation_time:
            self.close()

    def __close_simulator(self) -> None:
        """ Close the simulator process. """
        if self.__simulator:
            self.__simulator.terminate()
            self.__simulator.communicate()
            self.__simulator = None

    def __set_batsim_call_me_later(self, at: float) -> None:
        """ Setup a call me later request. """
        request = CallMeLaterBatsimRequest(self.current_time, at)
        if not any(isinstance(r, CallMeLaterBatsimRequest) and r.at == request.at for r in self.__batsim_requests):
            self.__batsim_requests.append(request)

    def __set_batsim_host_pstate(self, host_id: int, pstate_id: int) -> None:
        """ Set Batsim host power state. """
        def get_old_request() -> Optional[SetResourceStateBatsimRequest]:
            """ Get the request with the same properties. """
            for r in self.__batsim_requests:
                if r.timestamp == self.current_time and isinstance(r, SetResourceStateBatsimRequest) and r.state == pstate_id:
                    return r
            return None

        # We try to minimize the number of requests.
        request = get_old_request()

        if request:
            request.add_resource(host_id)
        else:
            request = SetResourceStateBatsimRequest(
                self.current_time, [host_id], pstate_id)
            self.__batsim_requests.append(request)

    def __handle_batsim_events(self) -> None:
        """ Handle Batsim events. """
        msg = self.__network.recv()
        for event in msg.events:
            self.__current_time = event.timestamp
            if event.type in self.__batsim_event_handlers:
                assert isinstance(event.type, BatsimEventType)
                self.__batsim_event_handlers[event.type](event)

        self.__current_time = msg.now

    def __send_requests(self) -> None:
        """ Send Batsim requests. """
        msg = BatsimMessage(self.current_time, self.__batsim_requests)
        self.__network.send(msg)
        self.__batsim_requests.clear()

    def __on_batsim_simulation_begins(self, event: SimulationBeginsBatsimEvent) -> None:
        self.__platform = event.platform

    def __on_batsim_simulation_ends(self, _) -> None:
        """ Handle batsim simulation ends event. """
        if self.__simulator:
            ack = BatsimMessage(self.current_time, [])
            self.__network.send(ack)
            self.__simulator.wait(5)
        self.close()

    def __on_batsim_host_state_changed(self, event: ResourcePowerStateChangedBatsimEvent) -> None:
        """ Handle batsim host state changed event. 

        When a host is switched on/off, the batsim simulates the transition costs
        and tells the scheduler only when the host is sleeping or idle. Thus, 
        Batsim is the responsible to tell when the host finished its transition.
        """
        assert self.__platform, "For some reason, the platform was not loaded."

        for h_id in event.resources:
            h = self.__platform.get_host(h_id)
            assert h.pstate

            if h.is_switching_off:
                h._set_off()
                self.__dispatch_event(HostEvent.STATE_CHANGED, h)
            elif h.is_switching_on:
                h._set_on()
                self.__dispatch_event(HostEvent.STATE_CHANGED, h)
            elif (h.is_idle or h.is_computing) and h.pstate.id != event.state:
                h._set_computation_pstate(event.state)
                self.__dispatch_event(
                    HostEvent.COMPUTATION_POWER_STATE_CHANGED, h)

            assert h.pstate.id == event.state, ('For some reason, the internal '
                                                'platform differs from the '
                                                'Batsim platform, got pstate '
                                                '{} while batsim got pstate {}.'
                                                ''.format(h.pstate.id, event.state))

        self.__start_runnable_jobs()

    def __on_batsim_requested_call(self, _) -> None:
        """ Handle batsim answer to call me back request.  """
        if self.current_time in self.__callbacks:
            for callback in self.__callbacks[self.current_time]:
                callback(self.current_time)
            del self.__callbacks[self.current_time]

    def __on_batsim_job_submitted(self, event: JobSubmittedBatsimEvent) -> None:
        """ Handle batsim job submitted event.  """
        self.__jobs.append(event.job)
        event.job._submit(self.current_time)
        self.__dispatch_event(JobEvent.SUBMITTED, event.job)

    def __on_batsim_job_completed(self, event: JobCompletedBatsimEvent) -> None:
        """ Handle batsim job submitted event.  """

        job = next((j for j in self.__jobs if j.id == event.job_id), None)
        assert job, "The job {} was not found.".format(event.job_id)
        assert job.allocation and self.__platform

        job._terminate(self.current_time, event.job_state)

        for h_id in job.allocation:
            host = self.__platform.get_host(h_id)
            host._release(job.id)
            self.__dispatch_event(HostEvent.STATE_CHANGED, host)

        if job.storage_mapping:
            for s_id in set(job.storage_mapping.values()):
                self.__platform.get_storage(s_id)._release(job.id)

        self.__jobs.remove(job)
        self.__dispatch_event(JobEvent.COMPLETED, job)
        self.__start_runnable_jobs()

    def __on_batsim_notify(self, event: NotifyBatsimEvent) -> None:
        """ Handle batsim submitter finished event.  """
        if event.notify_type == BatsimNotifyType.NO_MORE_STATIC_JOB_TO_SUBMIT:
            self.__no_more_jobs_to_submit = True
        elif event.notify_type == BatsimNotifyType.NO_MORE_EXTERNAL_EVENT_TO_OCCUR:
            self.__no_more_external_event_to_occur = True
        elif event.notify_type == BatsimNotifyType.EVENT_MACHINE_UNAVAILABLE:
            assert event.resources
            for res_id in event.resources:
                res = self.__platform.get(res_id)
                res._set_unavailable()
                if isinstance(res, Host):
                    self.__dispatch_event(HostEvent.STATE_CHANGED, res)
        elif event.notify_type == BatsimNotifyType.EVENT_MACHINE_AVAILABLE:
            assert event.resources
            for res_id in event.resources:
                res = self.__platform.get(res_id)
                res._set_available()
                if isinstance(res, Host):
                    self.__dispatch_event(HostEvent.STATE_CHANGED, res)

    def __on_sigterm(self, signum, frame) -> None:
        """ Close simulation on sigterm.  """
        self.__close_simulator()
        sys.exit(signum)
