from collections import defaultdict
from enum import Enum
from typing import Union
from typing import Iterator, Set
from typing import Optional
from typing import Sequence
from typing import List


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


class PowerState:
    """ This class describes a host power state (model).

    When the host is on, the energy consumption naturally depends on both the 
    current CPU load and the host energy profile. Following this principle, 
    we adopt a special case from the model used in SimGrid which, the host can 
    be idle (0% cpu) or at full load (100% cpu)

    Args:
        pstate_id: The power state id. Must be unique.
        pstate_type: The power state type.
        watt_idle: Consumption (Watts) when the host is up but without anything to do.
        watt_full: Consumption (Watts) when the host cpu is at 100%.

    Raises:
        AssertionError: In case of invalid arguments type.
        ValueError: In case of power profile values are not equal when the
            power state type is not a computation one or watts values are
            invalid.

    Examples:
        >>> ps1 = PowerState(0, PowerStateType.COMPUTATION, 90, 160)
        >>> ps2 = PowerState(0, PowerStateType.SLEEP, 10, 10)
    """

    def __init__(self,
                 pstate_id: int,
                 pstate_type: PowerStateType,
                 watt_idle: float,
                 watt_full: float) -> None:

        if watt_idle < 0:
            raise ValueError('Expected `watt_idle` argument to be greater '
                             'than or equal to zero, got {}'.format(watt_idle))

        if watt_full < 0:
            raise ValueError('Expected `watt_full` argument to be greater '
                             'than or equal to zero, got {}'.format(watt_full))

        if pstate_type != PowerStateType.COMPUTATION and watt_idle != watt_full:
            raise ValueError('Expected `watt_idle` and `watt_full` '
                             'arguments to have the same value when '
                             'power state type is not COMPUTATION, '
                             'got {} and {}.'.format(watt_idle, watt_full))

        assert isinstance(pstate_type, PowerStateType)
        self.__id = pstate_id
        self.__type = pstate_type
        self.__watt_idle = float(watt_idle)
        self.__watt_full = float(watt_full)

    def __str__(self) -> str:
        return "{}.{} ({})".format(self.type.__class__.__name__, str(self.type), self.id)

    def __repr__(self) -> str:
        return str(self)

    @property
    def id(self) -> int:
        """ The Power State ID """
        return self.__id

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


class Storage:
    """ This class describes a Batsim storage resource.

    Args:
        id: The storage id. Must be unique within a platform.
        name: The storage name.
        allow_sharing: Whether multiple jobs can share the storage. 
            Defaults to True.
        metadata: Extra storage properties that can be used by some functions
            beyond the scope of Batsim or Batsim-py. Defaults to None.
    """

    def __init__(self,
                 id: int,
                 name: str,
                 allow_sharing: bool = True,
                 metadata: Optional[dict] = None) -> None:
        self.__id = int(id)
        self.__name = str(name)
        self.__metadata = metadata
        self.__allow_sharing = allow_sharing
        self.__jobs: Set[str] = set()

    def __repr__(self) -> str:
        return f"Storage_{self.id}"

    def __str__(self) -> str:
        return f"Storage_{self.id}"

    @property
    def id(self) -> int:
        """ Storage ID """
        return self.__id

    @property
    def name(self) -> str:
        """ Storage name. """
        return self.__name

    @property
    def metadata(self) -> Optional[dict]:
        """ Storage extra properties. """
        if self.__metadata:
            return dict(self.__metadata)
        else:
            return None

    @property
    def jobs(self) -> List[str]:
        """ All jobs that are using this storage. """
        return list(self.__jobs)

    @property
    def is_allocated(self) -> bool:
        """ Whether the storage is being used by a job. """
        return bool(self.__jobs)

    @property
    def is_shareable(self) -> bool:
        """ Whether multiple jobs can share this storage. """
        return self.__allow_sharing

    def _allocate(self, job_id: str) -> None:
        """ Allocate storage for a job.

        This is an internal method to be used by the simulator only.

        Args:
            job_id: The job that will use this storage.

        Raises:
            RuntimeError: In case of storage is not shareable and is already 
                being used by another job.
        """
        if not self.is_shareable and self.is_allocated:
            raise RuntimeError('This storage cannot be used by multiple jobs.')

        self.__jobs.add(job_id)

    def _release(self, job_id: str) -> None:
        """ Release storage.

        This is an internal method to be used by the simulator only.

        Args:
            job_id: The job that will release this storage.
        """
        self.__jobs.remove(job_id)


class Host:
    """ This class describes a Batsim computing resource (host).

    A host is the resource on which a job can run.

    Args:
        id: The host id. Must be unique within a platform.
        name: The host name.
        pstates: The host power states. Defaults to None.
            A host can have several computing power states and only one sleep 
            and transition (On/Off) power states. Computing power states serves 
            as a way to implement different DVFS levels while the transition 
            power states are used only to simulate the costs of switching On/Off. 
            A host cannot be used when it's being switched On/Off or sleeping. 
            If you only want to implement DVFS, there is no need to provide a sleep 
            and a transition power state. Moreover, if you provide a sleeping power 
            state you must provide both transition power states (switching On/Off).
        allow_sharing: Whether multiple jobs can share the host. 
            Defaults to False.
        metadata: Extra host properties that can be used by some functions
            beyond the scope of Batsim or Batsim-py. Defaults to None.

    Raises:
        AssertionError: In case of invalid arguments type.
        ValueError: In case of missing pstates or the pstates ids are not unique.

    Examples:
        >>> ps1 = PowerState(0, PowerStateType.COMPUTATION, 10, 100)
        >>> ps2 = PowerState(1, PowerStateType.COMPUTATION, 15, 150)
        >>> h = Host(0, "Host_0", [ps1, ps2])
    """

    def __init__(self,
                 id: int,
                 name: str,
                 pstates: Optional[Sequence[PowerState]] = None,
                 allow_sharing: bool = False,
                 metadata: Optional[dict] = None) -> None:
        self.__id = int(id)
        self.__name = str(name)
        self.__state = HostState.IDLE
        self.__allow_sharing = allow_sharing
        self.__jobs: Set[str] = set()
        self.__pstates = None
        self.__current_pstate = None
        self.__metadata = metadata

        if pstates:
            if len(set(p.id for p in pstates)) != len(pstates):
                raise ValueError('Expected `pstates` argument to have unique '
                                 'ids, got {}.'.format(pstates))

            v: dict = defaultdict(int)
            for p in pstates:
                v[p.type] += 1

            if v[PowerStateType.COMPUTATION] == 0:
                raise ValueError('The host must have at least one COMPUTATION '
                                 'power state, got {}.'
                                 ''.format(v[PowerStateType.COMPUTATION]))

            nb = v[PowerStateType.SLEEP]
            nb += v[PowerStateType.SWITCHING_OFF]
            nb += v[PowerStateType.SWITCHING_ON]

            if nb > 0:
                if v[PowerStateType.SLEEP] != 1:
                    raise ValueError('The host sleeping power state was not '
                                     'defined or is not unique, got {}'
                                     ''.format(v[PowerStateType.SLEEP]))
                if v[PowerStateType.SWITCHING_OFF] != 1:
                    raise ValueError('The host switching off power state was '
                                     'not defined or is not unique, got {}'
                                     ''.format(v[PowerStateType.SWITCHING_OFF]))
                if v[PowerStateType.SWITCHING_ON] != 1:
                    raise ValueError('The host switching on power state was '
                                     'not defined or is not unique, got {}'
                                     ''.format(v[PowerStateType.SWITCHING_ON]))

            self.__pstates = tuple(sorted(pstates, key=lambda p: int(p.id)))
            self.__current_pstate = self.get_default_pstate()

    def __repr__(self) -> str:
        return "Host_%i" % self.id

    def __str__(self) -> str:
        return "Host_%i: %s" % (self.id, str(self.state))

    @property
    def id(self) -> int:
        """ The Host ID """
        return self.__id

    @property
    def name(self) -> str:
        """ Host name. """
        return self.__name

    @property
    def state(self) -> HostState:
        """ Host current state. """
        return self.__state

    @property
    def jobs(self) -> List[str]:
        """ All jobs id that are allocated in this host. """
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
    def metadata(self) -> Optional[dict]:
        """ Host extra properties. """
        if self.__metadata:
            return dict(self.__metadata)
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
    def is_shareable(self) -> bool:
        """ Whether multiple jobs can share this host. """
        return self.__allow_sharing

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

        ps = next((p for p in self.__pstates if p.id == pstate_id), None)
        if not ps:
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
        s_off_ps = self.get_pstate_by_type(PowerStateType.SWITCHING_OFF)
        s_ps = self.get_pstate_by_type(PowerStateType.SLEEP)
        assert s_off_ps and s_ps

        if self.is_allocated or not self.is_idle:
            raise RuntimeError('A host must be idle and free to be able to '
                               'switch off, got {}'.format(self.state))

        self.__set_pstate(s_off_ps[0])
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
            RuntimeError: In case of power states were not defined or it is 
                not a computation one or the current host state is not idle 
                nor computing.
            LookupError: In case of power state could not be found.
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
            SystemError: In case of the current host state is not switching off.
        """

        if not self.is_switching_off:
            raise SystemError('A host must be switching off to sleep, '
                              'got {}'.format(self.state))

        ps = self.get_pstate_by_type(PowerStateType.SLEEP)
        self.__set_pstate(ps[0])
        self.__set_state(HostState.SLEEPING)

    def _set_on(self) -> None:
        """ Finish the switching on routine and wake up.

        This is an internal method to be used by the simulator only.

        Raises:
            SystemError: In case of the current host state is not switching on.
        """

        if not self.is_switching_on:
            raise SystemError('A host must be switching on to wake up, '
                              'got {}'.format(self.state))

        ps = self.get_default_pstate()
        self.__set_pstate(ps)
        self.__set_state(HostState.IDLE)

    def _allocate(self, job_id: str) -> None:
        """ Allocate the host for a job.

        This is an internal method to be used by the simulator only.

        Args:
            job_id: The job that will use this host.

        Raises:
            RuntimeError: In case of host is not shareable and is already 
                being used by another job.
        """

        if not self.is_shareable and self.is_allocated:
            raise RuntimeError('This host cannot be used by multiple jobs.')

        self.__jobs.add(job_id)

    def _start_computing(self) -> None:
        """ Start computing.

        This is an internal method to be used by the simulator only.

        Raises:
            SystemError: In case of the current host power state is not a 
                computation one or there are no jobs allocated.
        """

        if not self.is_idle and not self.is_computing:
            raise SystemError('A host must be idle or computing to be able to '
                              'start a new job, got {}'.format(self.state))

        if not self.jobs:
            raise SystemError('The host must be allocated for a job '
                              'before start computing.')

        self.__set_state(HostState.COMPUTING)

    def _release(self, job_id: str) -> None:
        """ Release the resource.

        This is an internal method to be used by the simulator only.

        Args:
            job_id: The job that will release this host.
        """
        self.__jobs.remove(job_id)
        if not self.__jobs:
            self.__set_state(HostState.IDLE)

    def __set_pstate(self, new_pstate: PowerState) -> None:
        """ Set the power state.

        This is an internal method to be used by the host instance only.

        Raises:
            AssertionError: In case of invalid argument type.
        """
        assert self.__current_pstate
        assert isinstance(new_pstate, PowerState)
        self.__current_pstate = new_pstate

    def __set_state(self, new_state: HostState) -> None:
        """ Set the state.

        This is an internal method to be used by the host instance only.

        Raises:
            AssertionError: In case of invalid argument type.
        """
        assert isinstance(new_state, HostState)
        self.__state = new_state


class Platform:
    """ This class describes a platform.

    A platform is composed by a set of resources (computing, storage and network).

    Args:
        resources: the platform resources (hosts and storages).

    Raises:
        ValueError: In case of invalid platform size.
        SystemError: In case of invalid resources id.
    """

    def __init__(self, resources: Sequence[Union[Host, Storage]]) -> None:
        if not resources:
            raise ValueError("A platform must contain at least one resource.")

        if not all(h.id == i for i, h in enumerate(resources)):
            raise SystemError('The simulator expected resources id to '
                              'be a sequence starting from 0')
        self.__resources = tuple(sorted(resources, key=lambda h: h.id))

    @property
    def size(self) -> int:
        """ The number of resources in the platform. """
        return len(self.__resources)

    @property
    def power(self) -> float:
        """ The current consumption (in Watts). """
        return sum(h.power for h in self.hosts if h.power)

    @property
    def state(self) -> Sequence[HostState]:
        """ The current platform state. """
        return [h.state for h in self.hosts]

    @property
    def resources(self) -> Iterator[Union[Host, Storage]]:
        """ Platform resources. """
        return iter(self.__resources)

    @property
    def storages(self) -> Iterator[Storage]:
        """ Platform storage resources. """
        for r in self.__resources:
            if isinstance(r, Storage):
                yield r

    @property
    def hosts(self) -> Iterator[Host]:
        """ Platform computing resources. """
        for r in self.__resources:
            if isinstance(r, Host):
                yield r

    def get_available(self) -> Sequence[Host]:
        """ Get available hosts.

        An available host is the one that is not allocated for a job.

        Returns:
            A list with available hosts.
        """
        return list(filter(lambda h: not h.is_allocated, self.hosts))

    def get_host(self, host_id: int) -> Host:
        """ Get host by id.

        Args:
            host_id: The host id.

        Returns:
            The host with the corresponding id.

        Raises:
            LookupError: In case of resource not found or it's not 
                a computing resource.
        """
        if host_id >= self.size:
            raise LookupError(f"There're no resources with id {host_id}.")

        resource = self.__resources[host_id]
        if not isinstance(resource, Host):
            raise LookupError(f"A host with id {host_id} could not be "
                              f"found, got {resource}.")

        return resource

    def get_storage(self, storage_id: int) -> Storage:
        """ Get storage by id.

        Args:
            storage_id: The storage id.

        Returns:
            The storage with the corresponding id.

        Raises:
            LookupError: In case of resource not found or it's not 
                a storage resource.
        """
        if storage_id >= self.size:
            raise LookupError(f"There're no resources with id {storage_id}.")

        resource = self.__resources[storage_id]
        if not isinstance(resource, Storage):
            raise LookupError(f"A storage with id {storage_id} could not "
                              "be found, got {resource}.")

        return resource
