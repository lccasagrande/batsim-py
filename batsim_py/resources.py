from enum import Enum
from typing import Callable
from typing import Optional
from typing import Sequence
from typing import Union
from typing import List

from . import dispatcher
from .events import HostEvent
from .events import JobEvent
from .jobs import Job
from .utils.commons import Identifier


class HostState(Enum):
    """ Batsim Host State

    This class enumerates the distinct states a host can be in.
    """

    IDLE = 0
    SWITCHING_ON = 1
    SLEEPING = 2
    SWITCHING_OFF = 3
    COMPUTING = 4

    def __str__(self) -> str:
        return self.name


class PowerStateType(Enum):
    """ Batsim Host Power State Type

    This class enumerates the distinct power state types a host can be in.
    """
    SLEEP = 0
    COMPUTATION = 1
    SWITCHING_ON = 2
    SWITCHING_OFF = 3

    def __str__(self) -> str:
        return self.name


class PowerState(Identifier):
    """ This class describes a host power state (model).

    When the host is on, the energy consumption naturally depends on
    both the current CPU load and the host energy profile. We implement a
    special case from the model adopted in SimGrid which,
    the host can be idle (0% cpu) or at full load (100% cpu)

    Args:
        pstate_id: The power state id. Must be unique.
        pstate_type: The power state type.
        watt_idle: Consumption (Watts) when the host is up but without anything to do.
        watt_full: Consumption (Watts) when the host cpu is at 100%.

    Raises:
        TypeError: In case of invalid arguments.
        ValueError: In case of power profile values are not equal when the
            power state type is not a computation one.

    Examples:
        >>> ps = PowerState(0, PowerStateType.COMPUTATION, 90, 160)
    """

    def __init__(self,
                 pstate_id: int,
                 pstate_type: PowerStateType,
                 watt_idle: Union[int, float],
                 watt_full: Union[int, float]) -> None:

        if not isinstance(pstate_type, PowerStateType):
            raise TypeError('Expected `pstate_type` argument to be a '
                            '`PowerStateType` instance, got {}.'.format(pstate_type))

        if pstate_type != PowerStateType.COMPUTATION and watt_idle != watt_full:
            raise ValueError('Expected `watt_idle` and `watt_full` '
                             'arguments to have the same value when '
                             'power state type is not COMPUTATION, '
                             'got {} and {}.'.format(watt_idle, watt_full))

        super().__init__(int(pstate_id))
        self.__type = pstate_type
        self.__watt_idle = float(watt_idle)
        self.__watt_full = float(watt_full)

    def __str__(self) -> str:
        return "{}.{} ({})".format(self.type.__class__.__name__, str(self.type), self.id)

    def __repr__(self) -> str:
        return str(self)

    @property
    def watt_idle(self) -> float:
        """ Consumption (Watts) when the host is up but without anything to do. """
        return self.__watt_idle

    @property
    def watt_full(self) -> float:
        """ Consumption (Watts) when the host cpu is at 100%. """
        return self.__watt_full

    @property
    def type(self) -> PowerStateType:
        """ The power state type. """
        return self.__type


class Host(Identifier):
    """ This class describes a Batsim computing resource (host).

    A host is the resource on which a job can run.

    Args:
        id: The host id. Must be unique within a platform.
        name: The host name.
        pstates: The host power states. Defaults to None.
            A host can have several computing power states
            and only one sleep and transition (On/Off)
            power states. Computing power states serves as a way to
            implement different DVFS levels while the transition power states are
            only used to simulate the cost of switching On/Off. A host cannot
            compute jobs when it's being switched On/Off or sleeping. If you only
            want to control the DVFS, there is no need to provide a sleep and
            a transition power state.

    Raises:
        TypeError: In case of invalid arguments.

    Examples:
        >>> profile1 = PowerProfile(idle=90, full_load=190)
        >>> profile2 = PowerProfile(idle=120, full_load=250)
        >>> ps1 = PowerState(0, PowerStateType.COMPUTATION, profile1, 1e6)
        >>> ps2 = PowerState(1, PowerStateType.COMPUTATION, profile2, 2e6)
        >>> h = Host(0, "Host_0", [ps1, ps2])
    """

    def __init__(self,
                 id: int,
                 name: str,
                 pstates: Optional[Sequence[PowerState]] = None) -> None:
        if pstates and not all(isinstance(p, PowerState) for p in pstates):
            raise TypeError('Expected `pstates` argument to be a '
                            'sequence of `PowerState` instances, '
                            'got {}.'.format(pstates))

        super().__init__(int(id))
        self.__name = str(name)
        self.__state: HostState = HostState.IDLE
        self.__jobs: List[Job] = []
        self.__pstates = None
        self.__current_pstate = None

        if pstates:
            self.__pstates = tuple(sorted(pstates, key=lambda p: int(p.id)))
            self.__current_pstate = self.get_default_pstate()

    def __repr__(self) -> str:
        return "Host_%i" % self.id

    def __str__(self) -> str:
        return "Host_%i: %s" % (self.id, str(self.state))

    @property
    def name(self) -> str:
        """ Host name. """
        return self.__name

    @property
    def state(self) -> HostState:
        """ Host current state. """
        return self.__state

    @property
    def jobs(self) -> List[Job]:
        """ All jobs that were allocated. """
        return list(self.__jobs)

    @property
    def pstate(self) -> Optional[PowerState]:
        """ Host current power state. """
        return self.__current_pstate

    @property
    def pstates(self) -> Optional[List[PowerState]]:
        """ All power states of the host. """
        if self.__pstates:
            return list(self.__pstates)
        else:
            return None

    @property
    def is_allocated(self) -> bool:
        """ Whether this host was allocated for some job. """
        return bool(self.__jobs)

    @property
    def is_idle(self) -> bool:
        """ Whether this host is idle (0% cpu). """
        return self.__state == HostState.IDLE

    @property
    def is_computing(self) -> bool:
        """ Whether this host is computing (100% cpu). """
        return self.__state == HostState.COMPUTING

    @property
    def is_switching_off(self) -> bool:
        """ Whether this host is switching off. """
        return self.__state == HostState.SWITCHING_OFF

    @property
    def is_switching_on(self) -> bool:
        """ Whether this host is switching on. """
        return self.__state == HostState.SWITCHING_ON

    @property
    def is_sleeping(self) -> bool:
        """ Whether this host is sleeping. """
        return self.__state == HostState.SLEEPING

    @property
    def power(self) -> Optional[float]:
        """ Current host consumption (in Watts). """
        if not self.pstate:
            return None
        elif self.is_computing:
            return self.pstate.watt_full
        else:
            return self.pstate.watt_idle

    def get_pstate_by_type(self, ps_type: PowerStateType) -> List[PowerState]:
        """ Get a power state by type.

        Args:
            ps_type: The power state type.

        Raises:
            RuntimeError: In case of power states were not defined.
            LookupError: In case of power state could not be found.

        Returns:
            All power states with the corresponding power state type.
        """
        if not self.__pstates:
            raise RuntimeError('Cannot get undefined power states. Make sure the '
                               'power states are defined in the '
                               'platform description (XML).')

        pstates = [p for p in self.__pstates if p.type == ps_type]
        if not pstates:
            raise LookupError('Power states with type {} could not '
                              'be found.'.format(ps_type))

        return pstates

    def get_pstate_by_id(self, pstate_id: int) -> PowerState:
        """ Get a power state by id.

        Args:
            pstate_id: The power state id.

        Raises:
            LookupError: In case of power state could not be found.
            RuntimeError: In case of power states were not defined.

        Returns:
            The power state with id.
        """
        if not self.__pstates:
            raise RuntimeError('Cannot get undefined power states. Make sure the '
                               'power states are defined in the '
                               'platform description (XML).')
        try:
            ps = self.__pstates[pstate_id]
        except IndexError:
            raise LookupError('Power state with id {} could not '
                              'be found.'.format(pstate_id))
        return ps

    def get_sleep_pstate(self) -> PowerState:
        """ Get the host sleep power state.

        Raises:
            LookupError: In case of sleep power state could not be found.
            RuntimeError: In case of power states were not defined.

        Returns:
            The host sleep power state.
        """
        return self.get_pstate_by_type(PowerStateType.SLEEP)[0]

    def get_default_pstate(self) -> PowerState:
        """ Get the default host power state.

        The default power state is the first computation state provided which,
        are sorted by its id.

        Raises:
            LookupError: In case of default power state could not be found.
            RuntimeError: In case of power states were not defined.

        Returns:
            The host default power state.
        """
        return self.get_pstate_by_type(PowerStateType.COMPUTATION)[0]

    def _switch_off(self) -> None:
        """ Switch off the host.

        This is an internal method to be used by the simulator only.
        It simulates the sleeping routine and its time/cost.

        Raises:
            LookupError: In case of switching off or sleep power states
                could not be found.
            RuntimeError: In case of power states were not defined or the
                host cannot be switched off which, occurs when the host is
                not idle or it's allocated for a job.
        """
        ps = self.get_pstate_by_type(PowerStateType.SWITCHING_OFF)
        self.get_pstate_by_type(PowerStateType.SLEEP)  # Just assert

        if self.is_allocated or not self.is_idle:
            raise RuntimeError('A host must be idle and free to be able to '
                               'switch off, got {}'.format(self.state))

        self.__set_pstate(ps[0])
        self.__set_state(HostState.SWITCHING_OFF)

    def _switch_on(self) -> None:
        """ Switch on the host.

        This is an internal method to be used by the simulator only.
        It simulates the bootup routine and its time/cost.

        Raises:
            LookupError: In case of switching on power state could not be found.
            RuntimeError: In case of power states were not defined or
                the current host state is not sleeping.
        """
        ps = self.get_pstate_by_type(PowerStateType.SWITCHING_ON)

        if not self.is_sleeping:
            raise RuntimeError('A host must be sleeping to switch on, '
                               'got {}'.format(self.state))

        self.__set_pstate(ps[0])
        self.__set_state(HostState.SWITCHING_ON)

    def _set_computation_pstate(self, pstate_id: int) -> None:
        """ Set a computation power state.

        This is useful when DVFS is desired. It's an internal method to be
        used by the simulator only.

        Args:
            pstate_id: The power state id.

        Raises:
            RuntimeError: In case of power states were not defined.
            LookupError: In case of power state could not be found or
                it is not a computation one or the current host state is not
                idle nor computing.
        """

        next_ps = self.get_pstate_by_id(pstate_id)

        if next_ps.type != PowerStateType.COMPUTATION:
            raise RuntimeError('The power state is not a computation one, ',
                               'got {}'.format(next_ps))

        if not self.is_idle and not self.is_computing:
            raise RuntimeError('A host must be idle or computing to switch to '
                               'another computation power state, '
                               'got {}'.format(self.state))

        self.__set_pstate(next_ps)

    def _set_off(self) -> None:
        """ Finish the switching off routine and sleep.

        This is an internal method to be used by the simulator only.

        Raises:
            LookupError: In case of sleep power state could not be found.
            SystemError: In case of the current host state is not switching off or
                power state were not defined.
        """

        if not self.is_switching_off:
            SystemError('A host must be switching off to sleep, '
                        'got {}'.format(self.state))

        try:
            ps = self.get_pstate_by_type(PowerStateType.SLEEP)
        except RuntimeError:
            raise SystemError('Something bad happened and the simulator is '
                              'trying to set off an host without power states.')

        self.__set_pstate(ps[0])
        self.__set_state(HostState.SLEEPING)

    def _set_on(self) -> None:
        """ Finish the switching on routine and wake up.

        This is an internal method to be used by the simulator only.

        Raises:
            LookupError: In case of default power state could not be found.
            SystemError: In case of the current host state is not switching on or
                power states were not defined.
        """

        if not self.is_switching_on:
            SystemError('A host must be switching on to wake up, '
                        'got {}'.format(self.state))

        try:
            ps = self.get_default_pstate()
        except RuntimeError:
            raise SystemError('Something bad happened and the simulator is '
                              'trying to set on an host without power states.')

        self.__set_pstate(ps)
        self.__set_state(HostState.IDLE)

    def _allocate(self, job: Job) -> None:
        """ Allocate the host for a job.

        This is an internal method to be used by the simulator only.

        Args:
            job: The job that will use this host.
        """
        if any(j.id == job.id for j in self.__jobs):
            return

        self.__jobs.append(job)

        dispatcher.subscribe(self.__start_computing, JobEvent.STARTED, job)
        dispatcher.dispatch(HostEvent.ALLOCATED, self)

    def __start_computing(self, sender: Job) -> None:
        """ Start computing.

        This is an internal method to be used by the job instance only.
        When a job allocated for this host starts it'll dispatch an event,
        we catch this event to know if the host must start computing.

        Raises:
            SystemError: In case of the current host power state is not a
                computation one or the job was not allocated for this host.
        """

        if not self.is_idle or self.is_computing:
            raise SystemError('A host must be in idle or computing to be able to '
                              'start a new job, got {}'.format(self.state))
        if not any(j.id == sender.id for j in self.__jobs):
            raise SystemError('The host cannot find this job to execute, '
                              'got {}'.format(self.__jobs))

        dispatcher.subscribe(self.__release, JobEvent.COMPLETED, sender)
        self.__set_state(HostState.COMPUTING)

    def __release(self, sender: Job) -> None:
        """ Release the resource.

        This is an internal method to be used by the job instance only.
        When a job finish it'll dispatch an event, we catch this event to
        know if the host must deallocate it.

        Raises:
            SystemError: In case of the job could not be found.
        """
        try:
            self.__jobs.remove(sender)
        except ValueError:
            raise SystemError('The job could not be found, '
                              'got {}.'.format(self.__jobs))

        if not self.__jobs:
            self.__set_state(HostState.IDLE)

    def __set_pstate(self, new_pstate: PowerState) -> None:
        """ Set the power state and dispatch an event.

        This is an internal method to be used by the job instance only.

        Raises:
            TypeError: In case of the new power state is not an instance
                of PowerState.
        """
        assert self.__current_pstate

        if not isinstance(new_pstate, PowerState):
            raise TypeError('The new power state is not an instance of '
                            'the `PowerState` class, got {}.'.format(new_pstate))

        if self.__current_pstate.type == PowerStateType.COMPUTATION and new_pstate.type == PowerStateType.COMPUTATION:
            self.__current_pstate = new_pstate
            dispatcher.dispatch(
                HostEvent.COMPUTATION_POWER_STATE_CHANGED, self)
        else:
            self.__current_pstate = new_pstate

    def __set_state(self, new_state: HostState) -> None:
        """ Set the state and dispatch an event.

        This is an internal method to be used by the job instance only.

        Raises:
            TypeError: In case of the new state is not an instance
                of HostState.
        """

        if not isinstance(new_state, HostState):
            raise TypeError('The new state is not an instance of '
                            'the `HostState` class, got {}.'.format(new_state))

        if self.__state != new_state:
            self.__state = new_state
            dispatcher.dispatch(HostEvent.STATE_CHANGED, self)


class Platform:
    """ This class describes a platform.

    A platform is composed by a set of resources (computing, storage and network).

    Args:
        hosts: the computing resources.

    Raises:
        TypeError: In case of invalid arguments.
        ValueError: In case of invalid platform size.
    """

    def __init__(self, hosts: Sequence[Host]) -> None:
        if not all(isinstance(h, Host) for h in hosts):
            raise TypeError('A platform must contain a list of hosts of'
                            'type `Host`, got {}.'.format(hosts))

        if not hosts:
            raise ValueError("A platform must contain at least one host.")

        self.__hosts = {h.id: h for h in sorted(hosts, key=lambda h: h.id)}

    @property
    def size(self) -> int:
        """ The number of resources in the platform. """
        return len(self.__hosts)

    @property
    def power(self) -> float:
        """ The current consumption (in Watts). """
        return sum(h.power for h in self.__hosts.values() if h.power)

    @property
    def hosts(self) -> Sequence[Host]:
        """ The platform resources. """
        return list(self.__hosts.values())

    @property
    def state(self) -> Sequence[HostState]:
        """ The current platform state. """
        return [h.state for h in self.__hosts.values()]

    def get_all(self,
                filtr: Optional[Callable[[Host], bool]] = None) -> Sequence[Host]:
        """ Filter hosts.

        Args:
            filtr: A function that returns if the host must be included or not.

        Returns:
            A filtered list of hosts.
        """
        if filtr:
            filtered = [h for h_id, h in self.__hosts.items() if filtr(h)]
        else:
            filtered = list(self.__hosts.values())

        return filtered

    def get_available(self) -> Sequence[Host]:
        """ Get available hosts.

        An available host is a host that was not allocated for any job.

        Returns:
            A list of available hosts.
        """
        return self.get_all(lambda h: not h.is_allocated)

    def get(self, host_id: int) -> Host:
        """ Get host by id.

        Args:
            host_id: The host id.

        Returns:
            The host with the corresponding id.

        Raises:
            LookupError: In case of host could not be found.
        """
        host = self.__hosts.get(host_id, None)
        if not host:
            raise LookupError('Host with id {} was not found in '
                              'the platform.'.format(host_id))

        return host
