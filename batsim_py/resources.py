from enum import Enum

from pydispatch import dispatcher

from .utils.commons import Identifier
from .events import HostEvent
from .events import JobEvent


class HostState(Enum):
    """Host State """
    IDLE = 0
    SWITCHING_ON = 1
    SLEEPING = 2
    SWITCHING_OFF = 3
    COMPUTING = 4

    def __str__(self):
        return self.name


class PowerStateType(Enum):
    """Host Power State Types"""
    SLEEP = 0
    COMPUTATION = 1
    SWITCHING_ON = 2
    SWITCHING_OFF = 3

    def __str__(self):
        return self.name


class PowerProfile:
    """
        This is a class for the host power state profile

        It's a special case of the energy model adopted in SimGrid which,
        hosts can be idle or at full load only.
    """

    def __init__(self, idle, full_load):
        """ 
        Creates a PowerProfile instance. 

        Arguments: 
            idle (float): wattage when your host is up and running, but without anything to do.
            full_load (float): wattage when all cores of the host are at 100%.   
        Raises:
            AssertionError: In case of invalid arguments.
        """
        self.__idle = float(idle)
        self.__full_load = float(full_load)

    def __str__(self):
        return "Idle is %i Watts and AllCores is %i Watts" % (self.idle, self.full_load)

    @property
    def idle(self):
        return self.__idle

    @property
    def full_load(self):
        return self.__full_load


class PowerState(Identifier):
    """ Host Power State """

    def __init__(self, pstate_id, pstate_type, power_profile, pstate_speed):
        assert isinstance(pstate_type, PowerStateType)
        assert isinstance(power_profile, PowerProfile)
        if pstate_type != PowerStateType.COMPUTATION:
            assert power_profile.idle == power_profile.full_load, "Only computation power states can have different wattages."

        super().__init__(int(pstate_id))
        self.__type = pstate_type
        self.__power_profile = power_profile
        self.__speed = float(pstate_speed)

    def __str__(self):
        return "{}.{} ({})".format(self.type.__class__.__name__, str(self.type), self.id)

    def __repr__(self):
        return str(self)

    @property
    def speed(self):
        return self.__speed

    @property
    def power_profile(self):
        return self.__power_profile

    @property
    def type(self):
        return self.__type


class Host(Identifier):
    """ A host is the computing resource on which a job can run"""

    def __init__(self, id, name, pstates):
        super().__init__(int(id))
        self.__name = str(name)
        self.__pstates = sorted(pstates, key=lambda p: int(p.id))
        self.__state = HostState.IDLE
        self.__jobs = []
        self.__pstate = self.get_default_pstate()

    def __repr__(self):
        return "Host_%i" % self.id

    def __str__(self):
        return "Host_%i: %s" % (self.id, str(self.state))

    @property
    def power(self):
        return self.__pstate.power_profile.full_load if self.is_computing else self.__pstate.power_profile.idle

    @property
    def energy_to_switch_on(self):
        ps = self.get_switching_on_pstate()
        return (1 / ps.speed) * ps.power_profile.full_load

    @property
    def energy_to_switch_off(self):
        ps = self.get_switching_off_pstate()
        return (1 / ps.speed) * ps.power_profile.full_load

    @property
    def pstates(self):
        return list(self.__pstates)

    @property
    def jobs(self):
        return list(self.__jobs)

    @property
    def is_allocated(self):
        return bool(self.__jobs)

    @property
    def name(self):
        return self.__name

    @property
    def state(self):
        return self.__state

    @property
    def pstate(self):
        return self.__pstate

    @property
    def speed(self):
        return self.__pstate.speed

    @property
    def is_idle(self):
        return self.__state == HostState.IDLE

    @property
    def is_computing(self):
        return self.__state == HostState.COMPUTING

    @property
    def is_switching_off(self):
        return self.__state == HostState.SWITCHING_OFF

    @property
    def is_switching_on(self):
        return self.__state == HostState.SWITCHING_ON

    @property
    def is_sleeping(self):
        return self.__state == HostState.SLEEPING

    def get_pstate(self, pstate_id):
        return self.__pstates[pstate_id]

    def get_switching_off_pstate(self):
        return next(p for p in self.__pstates if p.type == PowerStateType.SWITCHING_OFF)

    def get_switching_on_pstate(self):
        return next(p for p in self.__pstates if p.type == PowerStateType.SWITCHING_ON)

    def get_sleep_pstate(self):
        return next(p for p in self.__pstates if p.type == PowerStateType.SLEEP)

    def get_default_pstate(self):
        return next(p for p in self.__pstates if p.type == PowerStateType.COMPUTATION)

    def _switch_off(self):
        assert not self.is_allocated and self.is_idle, "A host must be idle and free to be able to switch off."
        self.__set_pstate(self.get_switching_off_pstate())
        self.__set_state(HostState.SWITCHING_OFF)

    def _switch_on(self):
        assert self.is_sleeping, "A host must be sleeping to be able to switch on."
        self.__set_pstate(self.get_switching_on_pstate())
        self.__set_state(HostState.SWITCHING_ON)

    def _set_computation_pstate(self, pstate_id):
        assert self.__pstates[pstate_id].type == PowerStateType.COMPUTATION, "A host can only switch into computation power states."
        assert self.is_idle or self.is_computing, "A host must be idle or computing to change its computation power state."
        self.__set_pstate(self.__pstates[pstate_id])

    def _set_off(self):
        assert self.is_switching_off, "A host must be switching off to be able to sleep."
        self.__set_pstate(self.get_sleep_pstate())
        self.__set_state(HostState.SLEEPING)

    def _set_on(self):
        assert self.is_switching_on, "A host must be switching on to be able to be active."
        self.__set_pstate(self.get_default_pstate())
        self.__set_state(HostState.IDLE)

    def _allocate(self, job):
        if any(j.id == job.id for j in self.__jobs):
            return

        self.__jobs.append(job)

        dispatcher.connect(self.__start_computing,
                           signal=JobEvent.STARTED,
                           sender=job)

        self.__dispatch(HostEvent.ALLOCATED)

    def __start_computing(self, sender):
        assert self.__pstate.type == PowerStateType.COMPUTATION, "A host must be in a computation power state to start computing."
        assert any(
            j.id == sender.id for j in self.__jobs), "A host was not allocated for this job."

        dispatcher.connect(self.__release,
                           signal=JobEvent.COMPLETED,
                           sender=sender)

        self.__set_state(HostState.COMPUTING)

    def __release(self, sender):
        index = next(i for i, j in enumerate(self.__jobs) if j.id == sender.id)
        del self.__jobs[index]

        if not self.__jobs:
            self.__set_state(HostState.IDLE)

    def __set_pstate(self, new_pstate):
        assert isinstance(new_pstate, PowerState)

        if self.__pstate.type == PowerStateType.COMPUTATION and new_pstate.type == PowerStateType.COMPUTATION:
            self.__pstate = new_pstate
            self.__dispatch(HostEvent.COMPUTATION_POWER_STATE_CHANGED)
        else:
            self.__pstate = new_pstate

    def __set_state(self, new_state):
        assert isinstance(new_state, HostState)
        if self.__state != new_state:
            self.__state = new_state
            self.__dispatch(HostEvent.STATE_CHANGED)

    def __dispatch(self, event_type):
        dispatcher.send(signal=event_type, sender=self)


class Platform:
    def __init__(self, hosts):
        assert isinstance(
            hosts, list), "A platform must contain a list of hosts."
        assert len(hosts) > 0, "A platform must have at least one host."

        self.__hosts = {h.id: h for h in sorted(hosts, key=lambda h: h.id)}

    @property
    def size(self):
        return len(self.__hosts)

    @property
    def power(self):
        return sum(h.power for h in self.__hosts.values())

    @property
    def hosts(self):
        return list(self.__hosts.values())

    @property
    def state(self):
        return [h.state for h in self.__hosts.values()]

    def get_all(self, filtr=None):
        filtr = filtr or (lambda h: True)
        return [self.__hosts[h_id] for h_id in host_ids if filtr(self.__hosts[h_id])]

    def get_available(self):
        return self.get_all(lambda h: not h.is_allocated)

    def get(self, host_ids):
        host_ids = host_ids if isinstance(host_ids, list) else [host_ids]
        return [self.__hosts[h_id] for h_id in host_ids]
