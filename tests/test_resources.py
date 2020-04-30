import pytest

import batsim_py.dispatcher as dispatcher
from batsim_py.events import HostEvent
from batsim_py.jobs import DelayJobProfile
from batsim_py.jobs import Job
from batsim_py.jobs import JobState
from batsim_py.resources import Host
from batsim_py.resources import HostRole
from batsim_py.resources import HostState
from batsim_py.resources import Platform
from batsim_py.resources import PowerState
from batsim_py.resources import PowerStateType


class TestPowerState:
    def test_valid_instance(self):
        i, tp, idle, full = 0, PowerStateType.COMPUTATION, 100, 250
        p = PowerState(i, tp, idle, full)

        assert p.id == i and p.type == tp
        assert p.watt_idle == idle and p.watt_full == full

    def test_watt_idle_less_than_zero_must_raise(self):
        with pytest.raises(ValueError):
            PowerState(0, PowerStateType.COMPUTATION, -1, 10)

    def test_watt_full_less_than_zero_must_raise(self):
        with pytest.raises(ValueError):
            PowerState(0, PowerStateType.COMPUTATION, 1, -1)

    def test_watts_can_be_zero(self):
        p = PowerState(0, PowerStateType.COMPUTATION, 0, 0)
        assert p.watt_full == 0 and p.watt_idle == 0

    def test_sleep_with_different_watts_must_raise(self):
        with pytest.raises(ValueError):
            PowerState(0, PowerStateType.SLEEP, 1, 2)

    def test_switching_off_with_different_watts_must_raise(self):
        with pytest.raises(ValueError):
            PowerState(0, PowerStateType.SWITCHING_OFF, 1, 2)

    def test_switching_on_with_different_watts_must_raise(self):
        with pytest.raises(ValueError):
            PowerState(0, PowerStateType.SWITCHING_ON, 1, 2)


class TestHost:
    def test_valid_instance(self):
        p = PowerState(0, PowerStateType.COMPUTATION, 10, 10)
        i, n, r, p, m = 0, "n", HostRole.COMPUTE, [p], {'u': 1}
        h = Host(i, n, r, p, m)

        assert h.id == i and h.name == n and h.role == r
        assert h.pstates == p and h.metadata == m and not h.jobs

    def test_repr(self):
        assert repr(Host(0, "n")) == "Host_{}".format(0)

    def test_str(self):
        s = str(Host(0, "n"))
        assert s == "Host_{}: {}".format(0, str(HostState.IDLE))

    def test_initial_state_must_be_idle(self):
        p1 = PowerState(1, PowerStateType.COMPUTATION, 10, 100)
        h = Host(0, "n", pstates=[p1])
        assert h.state == HostState.IDLE and h.is_idle

    def test_pstates_must_have_computation(self):
        p1 = PowerState(1, PowerStateType.SLEEP, 100, 100)
        with pytest.raises(ValueError):
            Host(0, "n", pstates=[p1])

    def test_pstates_must_have_unique_id(self):
        p1 = PowerState(1, PowerStateType.SLEEP, 100, 100)
        p2 = PowerState(1, PowerStateType.COMPUTATION, 100, 100)
        with pytest.raises(ValueError):
            Host(0, "n", pstates=[p1, p2])

    def test_initial_pstate_must_be_the_first_computation(self):
        p1 = PowerState(0, PowerStateType.SLEEP, 10, 10)
        p2 = PowerState(1, PowerStateType.COMPUTATION, 10, 100)
        p3 = PowerState(2, PowerStateType.COMPUTATION, 1000, 10000)
        h = Host(0, "n", pstates=[p1, p3, p2])
        assert h.pstate == p2

    def test__pstates_not_defined(self):
        h = Host(0, "n")
        assert h.pstates is None

    def test_metadata_not_defined(self):
        h = Host(0, "n")
        assert h.metadata is None

    def test_metadata_valid(self):
        m = {"xx": 11}
        h = Host(0, "n", metadata=m)
        assert h.metadata == m

    def test_power_idle_state(self):
        p1 = PowerState(1, PowerStateType.COMPUTATION, 10, 100)
        h = Host(0, "n", pstates=[p1])
        assert h.power == 10 and h.is_idle

    def test_power_pstates_not_defined(self):
        h = Host(0, "n")
        assert h.power is None

    def test_power_computing_state(self):
        p1 = PowerState(1, PowerStateType.COMPUTATION, 10, 100)
        h = Host(0, "n", pstates=[p1])
        j = Job("n", "w", 1, DelayJobProfile("p", 100), 1)
        j._submit(0)
        h._allocate(j)
        j._allocate([h.id])
        j._start(10)
        assert h.power == 100 and h.is_computing

    def test_get_pstate_by_type_must_return_all_matches(self):
        p1 = PowerState(0, PowerStateType.SLEEP, 10, 10)
        p2 = PowerState(1, PowerStateType.COMPUTATION, 10, 100)
        p3 = PowerState(2, PowerStateType.COMPUTATION, 1000, 10000)
        h = Host(0, "n", pstates=[p1, p3, p2])
        pstates = h.get_pstate_by_type(PowerStateType.COMPUTATION)
        assert pstates == [p2, p3]

    def test_get_pstate_by_type_not_found_must_raise(self):
        p1 = PowerState(0, PowerStateType.SLEEP, 10, 10)
        p2 = PowerState(1, PowerStateType.COMPUTATION, 10, 100)
        p3 = PowerState(2, PowerStateType.COMPUTATION, 1000, 10000)
        h = Host(0, "n", pstates=[p1, p3, p2])

        with pytest.raises(LookupError):
            h.get_pstate_by_type(PowerStateType.SWITCHING_OFF)

    def test_get_pstate_by_type_not_defined_must_raise(self):
        h = Host(0, "n", pstates=[])
        with pytest.raises(RuntimeError):
            h.get_pstate_by_type(PowerStateType.SWITCHING_OFF)

    def test_get_pstate_by_id_valid(self):
        p1 = PowerState(0, PowerStateType.SLEEP, 10, 10)
        p2 = PowerState(2, PowerStateType.COMPUTATION, 10, 100)
        h = Host(0, "n", pstates=[p1, p2])
        assert h.get_pstate_by_id(2) == p2
        assert h.get_pstate_by_id(0) == p1

    def test_get_pstate_by_id_not_found_must_raise(self):
        p1 = PowerState(0, PowerStateType.SLEEP, 10, 10)
        p2 = PowerState(1, PowerStateType.COMPUTATION, 10, 100)
        h = Host(0, "n", pstates=[p1, p2])

        with pytest.raises(LookupError):
            h.get_pstate_by_id(2)

    def test_get_pstate_by_id_not_defined_must_raise(self):
        h = Host(0, "n", pstates=[])
        with pytest.raises(RuntimeError):
            h.get_pstate_by_id(0)

    def test_get_sleep_pstate_valid(self):
        p1 = PowerState(0, PowerStateType.SLEEP, 10, 10)
        p2 = PowerState(1, PowerStateType.COMPUTATION, 10, 100)
        h = Host(0, "n", pstates=[p1, p2])
        assert h.get_sleep_pstate() == p1

    def test_get_sleep_pstate_not_found_must_raise(self):
        p2 = PowerState(1, PowerStateType.COMPUTATION, 10, 100)
        h = Host(0, "n", pstates=[p2])
        with pytest.raises(LookupError):
            h.get_sleep_pstate()

    def test_get_sleep_pstate_not_defined_must_raise(self):
        h = Host(0, "n", pstates=[])
        with pytest.raises(RuntimeError):
            h.get_sleep_pstate()

    def test_get_default_pstate_valid(self):
        p1 = PowerState(0, PowerStateType.SLEEP, 10, 10)
        p2 = PowerState(1, PowerStateType.COMPUTATION, 10, 100)
        p3 = PowerState(2, PowerStateType.COMPUTATION, 1000, 10000)
        h = Host(0, "n", pstates=[p1, p3, p2])
        assert h.get_default_pstate() == p2

    def test_get_default_pstate_not_defined_must_raise(self):
        h = Host(0, "n", pstates=[])
        with pytest.raises(RuntimeError):
            h.get_default_pstate()

    def test_switch_off_valid(self):
        p1 = PowerState(0, PowerStateType.SLEEP, 10, 10)
        p2 = PowerState(1, PowerStateType.COMPUTATION, 10, 100)
        p3 = PowerState(2, PowerStateType.SWITCHING_OFF, 50, 50)
        h = Host(0, "n", pstates=[p1, p3, p2])

        h._switch_off()
        assert h.is_switching_off and h.power == p3.watt_idle
        assert h.pstate == p3 and h.state == HostState.SWITCHING_OFF

    def test_switch_off_not_dispatch_comp_pstate_changed_event(self):
        def foo(sender):
            self.__called = True

        self.__called = False
        dispatcher.subscribe(foo, HostEvent.COMPUTATION_POWER_STATE_CHANGED)
        p1 = PowerState(0, PowerStateType.SLEEP, 10, 10)
        p2 = PowerState(1, PowerStateType.COMPUTATION, 10, 100)
        p3 = PowerState(2, PowerStateType.SWITCHING_OFF, 50, 50)
        h = Host(0, "n", pstates=[p1, p3, p2])
        h._switch_off()
        assert not self.__called

    def test_switch_off_dispatch_event(self):
        def foo(sender):
            self.__called = True

        self.__called = False
        dispatcher.subscribe(foo, HostEvent.STATE_CHANGED)
        p1 = PowerState(0, PowerStateType.SLEEP, 10, 10)
        p2 = PowerState(1, PowerStateType.COMPUTATION, 10, 100)
        p3 = PowerState(2, PowerStateType.SWITCHING_OFF, 50, 50)
        h = Host(0, "n", pstates=[p1, p3, p2])
        h._switch_off()
        assert self.__called

    def test_switch_off_without_sleep_defined_must_raise(self):
        p2 = PowerState(1, PowerStateType.COMPUTATION, 10, 100)
        h = Host(0, "n", pstates=[p2])

        with pytest.raises(LookupError):
            h._switch_off()

    def test_switch_off_without_switching_off_defined_must_raise(self):
        p1 = PowerState(0, PowerStateType.SLEEP, 10, 10)
        p2 = PowerState(1, PowerStateType.COMPUTATION, 10, 100)
        h = Host(0, "n", pstates=[p1, p2])

        with pytest.raises(LookupError):
            h._switch_off()

    def test_switch_off_without_pstates_defined_must_raise(self):
        h = Host(0, "n", pstates=[])

        with pytest.raises(RuntimeError):
            h._switch_off()

    def test_switch_off_already_switching_off_must_raises(self):
        p1 = PowerState(0, PowerStateType.SLEEP, 10, 10)
        p2 = PowerState(1, PowerStateType.COMPUTATION, 10, 100)
        p3 = PowerState(2, PowerStateType.SWITCHING_OFF, 50, 50)
        h = Host(0, "n", pstates=[p1, p3, p2])

        h._switch_off()
        with pytest.raises(RuntimeError):
            h._switch_off()

    def test_switch_off_when_off_must_raises(self):
        p1 = PowerState(0, PowerStateType.SLEEP, 10, 10)
        p2 = PowerState(1, PowerStateType.COMPUTATION, 10, 100)
        p3 = PowerState(2, PowerStateType.SWITCHING_OFF, 50, 50)
        h = Host(0, "n", pstates=[p1, p3, p2])

        h._switch_off()
        h._set_off()
        with pytest.raises(RuntimeError):
            h._switch_off()

    def test_switch_off_switching_on_must_raises(self):
        p1 = PowerState(0, PowerStateType.SLEEP, 10, 10)
        p2 = PowerState(1, PowerStateType.COMPUTATION, 10, 100)
        p3 = PowerState(2, PowerStateType.SWITCHING_OFF, 50, 50)
        p4 = PowerState(3, PowerStateType.SWITCHING_ON, 50, 50)
        h = Host(0, "n", pstates=[p1, p3, p2, p4])

        h._switch_off()
        h._set_off()
        h._switch_on()
        with pytest.raises(RuntimeError):
            h._switch_off()

    def test_switch_off_computing_must_raise(self):
        p1 = PowerState(0, PowerStateType.SLEEP, 10, 10)
        p2 = PowerState(1, PowerStateType.COMPUTATION, 10, 100)
        p3 = PowerState(2, PowerStateType.SWITCHING_OFF, 50, 50)
        h = Host(0, "n", pstates=[p1, p3, p2])
        j = Job("n", "w", 1, DelayJobProfile("p", 100), 1)
        j._submit(0)
        h._allocate(j)
        j._allocate([h.id])
        j._start(10)

        with pytest.raises(RuntimeError):
            h._switch_off()

    def test_switch_off_allocated_must_raise(self):
        p1 = PowerState(0, PowerStateType.SLEEP, 10, 10)
        p2 = PowerState(1, PowerStateType.COMPUTATION, 10, 100)
        p3 = PowerState(2, PowerStateType.SWITCHING_OFF, 50, 50)
        h = Host(0, "n", pstates=[p1, p3, p2])
        j = Job("n", "w", 1, DelayJobProfile("p", 100), 1)
        j._submit(0)
        h._allocate(j)
        j._allocate([h.id])
        with pytest.raises(RuntimeError):
            h._switch_off()

    def test_switch_on_valid(self):
        p1 = PowerState(0, PowerStateType.SLEEP, 10, 10)
        p2 = PowerState(1, PowerStateType.COMPUTATION, 10, 100)
        p3 = PowerState(2, PowerStateType.SWITCHING_OFF, 50, 50)
        p4 = PowerState(3, PowerStateType.SWITCHING_ON, 75, 75)
        h = Host(0, "n", pstates=[p1, p3, p2, p4])

        h._switch_off()
        h._set_off()
        h._switch_on()
        assert h.is_switching_on and h.power == p4.watt_idle
        assert h.pstate == p4 and h.state == HostState.SWITCHING_ON

    def test_switch_on_not_dispatch_comp_pstate_changed_event(self):
        def foo(sender):
            self.__called = True

        self.__called = False
        dispatcher.subscribe(foo, HostEvent.COMPUTATION_POWER_STATE_CHANGED)
        p1 = PowerState(0, PowerStateType.SLEEP, 10, 10)
        p2 = PowerState(1, PowerStateType.COMPUTATION, 10, 100)
        p3 = PowerState(2, PowerStateType.SWITCHING_OFF, 50, 50)
        p4 = PowerState(3, PowerStateType.SWITCHING_ON, 75, 75)
        h = Host(0, "n", pstates=[p1, p3, p2, p4])

        h._switch_off()
        h._set_off()
        h._switch_on()
        assert not self.__called

    def test_switch_on_dispatch_event(self):
        def foo(sender):
            self.__called = True

        self.__called = False
        dispatcher.subscribe(foo, HostEvent.STATE_CHANGED)
        p1 = PowerState(0, PowerStateType.SLEEP, 10, 10)
        p2 = PowerState(1, PowerStateType.COMPUTATION, 10, 100)
        p3 = PowerState(2, PowerStateType.SWITCHING_OFF, 50, 50)
        p4 = PowerState(3, PowerStateType.SWITCHING_ON, 75, 75)
        h = Host(0, "n", pstates=[p1, p3, p2, p4])
        h._switch_off()
        h._set_off()
        h._switch_on()
        assert self.__called

    def test_switch_on_without_pstate_defined_must_raise(self):
        p1 = PowerState(0, PowerStateType.SLEEP, 10, 10)
        p2 = PowerState(1, PowerStateType.COMPUTATION, 10, 100)
        p3 = PowerState(2, PowerStateType.SWITCHING_OFF, 50, 50)
        h = Host(0, "n", pstates=[p1, p3, p2])

        h._switch_off()
        h._set_off()
        with pytest.raises(LookupError):
            h._switch_on()

    def test_switch_on_already_switching_on_must_raises(self):
        p1 = PowerState(0, PowerStateType.SLEEP, 10, 10)
        p2 = PowerState(1, PowerStateType.COMPUTATION, 10, 100)
        p3 = PowerState(2, PowerStateType.SWITCHING_OFF, 50, 50)
        p4 = PowerState(3, PowerStateType.SWITCHING_ON, 75, 75)
        h = Host(0, "n", pstates=[p1, p3, p2, p4])

        h._switch_off()
        h._set_off()
        h._switch_on()
        with pytest.raises(RuntimeError):
            h._switch_on()

    def test_switch_on_switching_off_must_raises(self):
        p1 = PowerState(0, PowerStateType.SLEEP, 10, 10)
        p2 = PowerState(1, PowerStateType.COMPUTATION, 10, 100)
        p3 = PowerState(2, PowerStateType.SWITCHING_OFF, 50, 50)
        p4 = PowerState(3, PowerStateType.SWITCHING_ON, 75, 75)
        h = Host(0, "n", pstates=[p1, p3, p2, p4])

        h._switch_off()
        with pytest.raises(RuntimeError):
            h._switch_on()

    def test_switch_on_idle_must_raises(self):
        p1 = PowerState(0, PowerStateType.SLEEP, 10, 10)
        p2 = PowerState(1, PowerStateType.COMPUTATION, 10, 100)
        p3 = PowerState(2, PowerStateType.SWITCHING_OFF, 50, 50)
        p4 = PowerState(3, PowerStateType.SWITCHING_ON, 75, 75)
        h = Host(0, "n", pstates=[p1, p3, p2, p4])
        with pytest.raises(RuntimeError):
            h._switch_on()

    def test_switch_on_computing_must_raise(self):
        p1 = PowerState(0, PowerStateType.SLEEP, 10, 10)
        p2 = PowerState(1, PowerStateType.COMPUTATION, 10, 100)
        p3 = PowerState(2, PowerStateType.SWITCHING_OFF, 50, 50)
        p4 = PowerState(3, PowerStateType.SWITCHING_ON, 75, 75)
        h = Host(0, "n", pstates=[p1, p3, p2, p4])

        j = Job("n", "w", 1, DelayJobProfile("p", 100), 1)
        j._submit(0)
        h._allocate(j)
        j._allocate([h.id])
        j._start(10)

        with pytest.raises(RuntimeError):
            h._switch_on()

    def test_switch_on_allocated_must_raise(self):
        p1 = PowerState(0, PowerStateType.SLEEP, 10, 10)
        p2 = PowerState(1, PowerStateType.COMPUTATION, 10, 100)
        p3 = PowerState(2, PowerStateType.SWITCHING_OFF, 50, 50)
        p4 = PowerState(3, PowerStateType.SWITCHING_ON, 75, 75)
        h = Host(0, "n", pstates=[p1, p3, p2, p4])

        j = Job("n", "w", 1, DelayJobProfile("p", 100), 1)
        j._submit(0)
        h._allocate(j)
        j._allocate([h.id])
        with pytest.raises(RuntimeError):
            h._switch_on()

    def test_set_computation_state_valid(self):
        p1 = PowerState(0, PowerStateType.COMPUTATION, 10, 100)
        p2 = PowerState(1, PowerStateType.COMPUTATION, 15, 150)
        h = Host(0, "n", pstates=[p1, p2])
        h._set_computation_pstate(1)

        assert h.pstate == p2

    def test_set_computation_state_dispatch_event(self):
        def foo(sender):
            self.__called = True

        self.__called = False
        dispatcher.subscribe(foo, HostEvent.COMPUTATION_POWER_STATE_CHANGED)
        p1 = PowerState(0, PowerStateType.COMPUTATION, 10, 100)
        p2 = PowerState(1, PowerStateType.COMPUTATION, 15, 150)
        h = Host(0, "n", pstates=[p1, p2])
        h._set_computation_pstate(1)
        assert self.__called

    def test_set_computation_state_not_found_must_raise(self):
        p1 = PowerState(0, PowerStateType.COMPUTATION, 10, 100)
        p2 = PowerState(1, PowerStateType.COMPUTATION, 15, 150)
        h = Host(0, "n", pstates=[p1, p2])
        with pytest.raises(LookupError):
            h._set_computation_pstate(2)

    def test_set_computation_state_not_defined_must_raise(self):
        h = Host(0, "n", pstates=[])
        with pytest.raises(RuntimeError):
            h._set_computation_pstate(2)

    def test_set_computation_state_not_correct_type_must_raise(self):
        p1 = PowerState(0, PowerStateType.SLEEP, 10, 10)
        p2 = PowerState(1, PowerStateType.COMPUTATION, 15, 150)
        h = Host(0, "n", pstates=[p1, p2])
        with pytest.raises(RuntimeError):
            h._set_computation_pstate(0)

    def test_set_computation_state_when_switching_off_must_raise(self):
        p1 = PowerState(0, PowerStateType.SLEEP, 10, 10)
        p2 = PowerState(1, PowerStateType.COMPUTATION, 10, 100)
        p3 = PowerState(2, PowerStateType.COMPUTATION, 15, 150)
        p4 = PowerState(3, PowerStateType.SWITCHING_OFF, 50, 50)
        p5 = PowerState(4, PowerStateType.SWITCHING_ON, 75, 75)
        h = Host(0, "n", pstates=[p1, p3, p2, p4, p5])
        h._switch_off()
        with pytest.raises(RuntimeError):
            h._set_computation_pstate(2)

    def test_set_computation_state_when_off_must_raise(self):
        p1 = PowerState(0, PowerStateType.SLEEP, 10, 10)
        p2 = PowerState(1, PowerStateType.COMPUTATION, 10, 100)
        p3 = PowerState(2, PowerStateType.COMPUTATION, 15, 150)
        p4 = PowerState(3, PowerStateType.SWITCHING_OFF, 50, 50)
        p5 = PowerState(4, PowerStateType.SWITCHING_ON, 75, 75)
        h = Host(0, "n", pstates=[p1, p3, p2, p4, p5])
        h._switch_off()
        h._set_off()
        with pytest.raises(RuntimeError):
            h._set_computation_pstate(2)

    def test_set_computation_state_when_switching_on_must_raise(self):
        p1 = PowerState(0, PowerStateType.SLEEP, 10, 10)
        p2 = PowerState(1, PowerStateType.COMPUTATION, 10, 100)
        p3 = PowerState(2, PowerStateType.COMPUTATION, 15, 150)
        p4 = PowerState(3, PowerStateType.SWITCHING_OFF, 50, 50)
        p5 = PowerState(4, PowerStateType.SWITCHING_ON, 75, 75)
        h = Host(0, "n", pstates=[p1, p3, p2, p4, p5])
        h._switch_off()
        h._set_off()
        h._switch_on()
        with pytest.raises(RuntimeError):
            h._set_computation_pstate(2)

    def test_set_computation_state_when_computing_valid(self):
        p1 = PowerState(0, PowerStateType.SLEEP, 10, 10)
        p2 = PowerState(1, PowerStateType.COMPUTATION, 10, 100)
        p3 = PowerState(2, PowerStateType.COMPUTATION, 15, 150)
        p4 = PowerState(3, PowerStateType.SWITCHING_OFF, 50, 50)
        p5 = PowerState(4, PowerStateType.SWITCHING_ON, 75, 75)
        h = Host(0, "n", pstates=[p1, p3, p2, p4, p5])

        j = Job("n", "w", 1, DelayJobProfile("p", 100), 1)
        j._submit(0)
        h._allocate(j)
        j._allocate([h.id])
        j._start(10)

        h._set_computation_pstate(2)
        assert h.pstate == p3 and h.power == p3.watt_full

    def test_set_off_valid(self):
        p1 = PowerState(0, PowerStateType.SLEEP, 10, 10)
        p2 = PowerState(2, PowerStateType.COMPUTATION, 15, 150)
        p3 = PowerState(3, PowerStateType.SWITCHING_OFF, 50, 50)
        h = Host(0, "n", pstates=[p1, p3, p2])
        h._switch_off()
        h._set_off()

        assert h.is_sleeping and h.power == p1.watt_full
        assert h.state == HostState.SLEEPING and h.pstate == p1

    def test_set_off_dispatch_event(self):
        def foo(sender):
            self.__called = True

        self.__called = False
        dispatcher.subscribe(foo, HostEvent.STATE_CHANGED)
        p1 = PowerState(0, PowerStateType.SLEEP, 10, 10)
        p2 = PowerState(2, PowerStateType.COMPUTATION, 15, 150)
        p3 = PowerState(3, PowerStateType.SWITCHING_OFF, 50, 50)
        h = Host(0, "n", pstates=[p1, p3, p2])
        h._switch_off()
        h._set_off()
        assert self.__called

    def test_set_off_when_idle_must_raise(self):
        p1 = PowerState(0, PowerStateType.SLEEP, 10, 10)
        p2 = PowerState(2, PowerStateType.COMPUTATION, 15, 150)
        p3 = PowerState(3, PowerStateType.SWITCHING_OFF, 50, 50)
        h = Host(0, "n", pstates=[p1, p3, p2])
        with pytest.raises(SystemError):
            h._set_off()

    def test_set_off_when_computing_must_raise(self):
        p1 = PowerState(0, PowerStateType.SLEEP, 10, 10)
        p2 = PowerState(2, PowerStateType.COMPUTATION, 15, 150)
        p3 = PowerState(3, PowerStateType.SWITCHING_OFF, 50, 50)
        h = Host(0, "n", pstates=[p1, p3, p2])

        j = Job("n", "w", 1, DelayJobProfile("p", 100), 1)
        j._submit(0)
        h._allocate(j)
        j._allocate([h.id])
        j._start(10)

        with pytest.raises(SystemError):
            h._set_off()

    def test_set_off_when_switching_on_must_raise(self):
        p1 = PowerState(0, PowerStateType.SLEEP, 10, 10)
        p2 = PowerState(1, PowerStateType.COMPUTATION, 10, 100)
        p3 = PowerState(3, PowerStateType.SWITCHING_OFF, 50, 50)
        p4 = PowerState(4, PowerStateType.SWITCHING_ON, 75, 75)
        h = Host(0, "n", pstates=[p1, p3, p2, p4])
        h._switch_off()
        h._set_off()
        h._switch_on()
        with pytest.raises(SystemError):
            h._set_off()

    def test_set_off_when_off_must_raise(self):
        p1 = PowerState(0, PowerStateType.SLEEP, 10, 10)
        p2 = PowerState(1, PowerStateType.COMPUTATION, 10, 100)
        p3 = PowerState(3, PowerStateType.SWITCHING_OFF, 50, 50)
        h = Host(0, "n", pstates=[p1, p3, p2])
        h._switch_off()
        h._set_off()
        with pytest.raises(SystemError):
            h._set_off()

    def test_set_on_valid(self):
        p1 = PowerState(0, PowerStateType.SLEEP, 10, 10)
        p2 = PowerState(1, PowerStateType.COMPUTATION, 15, 150)
        p3 = PowerState(2, PowerStateType.COMPUTATION, 10, 100)
        p4 = PowerState(3, PowerStateType.SWITCHING_OFF, 50, 50)
        p5 = PowerState(4, PowerStateType.SWITCHING_ON, 75, 75)
        h = Host(0, "n", pstates=[p1, p3, p2, p4, p5])
        h._switch_off()
        h._set_off()
        h._switch_on()
        h._set_on()

        assert h.is_idle and h.pstate == p2 and h.state == HostState.IDLE

    def test_set_on_dispatch_event(self):
        def foo(sender):
            self.__called = True

        self.__called = False
        dispatcher.subscribe(foo, HostEvent.STATE_CHANGED)
        p1 = PowerState(0, PowerStateType.SLEEP, 10, 10)
        p2 = PowerState(1, PowerStateType.COMPUTATION, 15, 150)
        p3 = PowerState(2, PowerStateType.COMPUTATION, 10, 100)
        p4 = PowerState(3, PowerStateType.SWITCHING_OFF, 50, 50)
        p5 = PowerState(4, PowerStateType.SWITCHING_ON, 75, 75)
        h = Host(0, "n", pstates=[p1, p3, p2, p4, p5])
        h._switch_off()
        h._set_off()
        h._switch_on()
        h._set_on()
        assert self.__called

    def test_set_on_when_computing_must_raise(self):
        p1 = PowerState(0, PowerStateType.SLEEP, 10, 10)
        p2 = PowerState(1, PowerStateType.COMPUTATION, 10, 100)
        p3 = PowerState(2, PowerStateType.SWITCHING_OFF, 50, 50)
        p4 = PowerState(3, PowerStateType.SWITCHING_ON, 75, 75)
        h = Host(0, "n", pstates=[p1, p3, p2, p4])
        j = Job("n", "w", 1, DelayJobProfile("p", 100), 1)
        j._submit(0)
        h._allocate(j)
        j._allocate([h.id])
        j._start(10)
        with pytest.raises(SystemError):
            h._set_on()

    def test_set_on_when_idle_must_raise(self):
        p1 = PowerState(0, PowerStateType.SLEEP, 10, 10)
        p2 = PowerState(1, PowerStateType.COMPUTATION, 10, 100)
        p3 = PowerState(2, PowerStateType.SWITCHING_OFF, 50, 50)
        p4 = PowerState(3, PowerStateType.SWITCHING_ON, 75, 75)
        h = Host(0, "n", pstates=[p1, p3, p2, p4])
        with pytest.raises(SystemError):
            h._set_on()

    def test_set_on_when_switching_off_must_raise(self):
        p1 = PowerState(0, PowerStateType.SLEEP, 10, 10)
        p2 = PowerState(1, PowerStateType.COMPUTATION, 10, 100)
        p3 = PowerState(2, PowerStateType.SWITCHING_OFF, 50, 50)
        p4 = PowerState(3, PowerStateType.SWITCHING_ON, 75, 75)
        h = Host(0, "n", pstates=[p1, p3, p2, p4])
        h._switch_off()
        with pytest.raises(SystemError):
            h._set_on()

    def test_set_on_when_off_must_raise(self):
        p1 = PowerState(0, PowerStateType.SLEEP, 10, 10)
        p2 = PowerState(1, PowerStateType.COMPUTATION, 10, 100)
        p3 = PowerState(2, PowerStateType.SWITCHING_OFF, 50, 50)
        p4 = PowerState(3, PowerStateType.SWITCHING_ON, 75, 75)
        h = Host(0, "n", pstates=[p1, p3, p2, p4])
        h._switch_off()
        h._set_off()
        with pytest.raises(SystemError):
            h._set_on()

    def test_allocate_valid(self):
        p1 = PowerState(1, PowerStateType.COMPUTATION, 10, 100)
        h = Host(0, "n", pstates=[p1])
        j = Job("n", "w", 1, DelayJobProfile("p", 100), 1)
        h._allocate(j)
        assert h.is_idle and h.jobs and h.jobs[0] == j

    def test_allocate_dispatch_event(self):
        def foo(sender):
            self.__called = True

        self.__called = False
        dispatcher.subscribe(foo, HostEvent.ALLOCATED)
        p1 = PowerState(1, PowerStateType.COMPUTATION, 10, 100)
        h = Host(0, "n", pstates=[p1])
        j = Job("n", "w", 1, DelayJobProfile("p", 100), 1)
        h._allocate(j)
        assert self.__called

    def test_allocate_multiple_jobs_valid(self):
        p1 = PowerState(1, PowerStateType.COMPUTATION, 10, 100)
        h = Host(0, "n", pstates=[p1])
        j1 = Job("j1", "w", 1, DelayJobProfile("p", 100), 1)
        j2 = Job("j2", "w", 1, DelayJobProfile("p", 100), 1)
        h._allocate(j1)
        h._allocate(j2)
        assert h.is_idle and h.jobs
        assert h.jobs[0] == j1
        assert h.jobs[1] == j2

    def test_allocate_job_twice_must_not_raise(self):
        h = Host(0, "n")
        j1 = Job("j1", "w", 1, DelayJobProfile("p", 100), 1)
        h._allocate(j1)
        h._allocate(j1)
        assert h.jobs and len(h.jobs) == 1

    def test_start_computing_valid(self):
        p1 = PowerState(1, PowerStateType.COMPUTATION, 10, 100)
        h = Host(0, "n", pstates=[p1])
        j = Job("n", "w", 1, DelayJobProfile("p", 100), 1)
        h._allocate(j)
        j._submit(1)
        j._allocate([h.id])
        j._start(1)
        assert h.is_computing and h.state == HostState.COMPUTING

    def test_start_computing_dispatch_event(self):
        def foo(sender):
            self.__called = True

        self.__called = False
        dispatcher.subscribe(foo, HostEvent.STATE_CHANGED)
        p1 = PowerState(1, PowerStateType.COMPUTATION, 10, 100)
        h = Host(0, "n", pstates=[p1])
        j = Job("n", "w", 1, DelayJobProfile("p", 100), 1)
        h._allocate(j)
        j._submit(1)
        j._allocate([h.id])
        j._start(1)
        assert self.__called

    def test_start_computing_when_off_must_raise(self):
        p1 = PowerState(0, PowerStateType.SLEEP, 10, 10)
        p2 = PowerState(1, PowerStateType.COMPUTATION, 10, 100)
        p3 = PowerState(2, PowerStateType.SWITCHING_OFF, 50, 50)
        p4 = PowerState(3, PowerStateType.SWITCHING_ON, 75, 75)
        h = Host(0, "n", pstates=[p1, p3, p2, p4])
        h._switch_off()
        h._set_off()

        j = Job("n", "w", 1, DelayJobProfile("p", 100), 1)
        h._allocate(j)
        j._submit(1)
        j._allocate([h.id])
        with pytest.raises(SystemError):
            j._start(1)

    def test_start_computing_when_switching_off_must_raise(self):
        p1 = PowerState(0, PowerStateType.SLEEP, 10, 10)
        p2 = PowerState(1, PowerStateType.COMPUTATION, 10, 100)
        p3 = PowerState(2, PowerStateType.SWITCHING_OFF, 50, 50)
        p4 = PowerState(3, PowerStateType.SWITCHING_ON, 75, 75)
        h = Host(0, "n", pstates=[p1, p3, p2, p4])
        h._switch_off()

        j = Job("n", "w", 1, DelayJobProfile("p", 100), 1)
        h._allocate(j)
        j._submit(1)
        j._allocate([h.id])
        with pytest.raises(SystemError):
            j._start(1)

    def test_start_computing_when_switching_on_must_raise(self):
        p1 = PowerState(0, PowerStateType.SLEEP, 10, 10)
        p2 = PowerState(1, PowerStateType.COMPUTATION, 10, 100)
        p3 = PowerState(2, PowerStateType.SWITCHING_OFF, 50, 50)
        p4 = PowerState(3, PowerStateType.SWITCHING_ON, 75, 75)
        h = Host(0, "n", pstates=[p1, p3, p2, p4])
        h._switch_off()
        h._set_off()
        h._switch_on()

        j = Job("n", "w", 1, DelayJobProfile("p", 100), 1)
        h._allocate(j)
        j._submit(1)
        j._allocate([h.id])
        with pytest.raises(SystemError):
            j._start(1)

    def test_start_computing_when_computing_valid(self):
        h = Host(0, "n")
        j1 = Job("n1", "w", 1, DelayJobProfile("p", 100), 1)
        j2 = Job("n2", "w", 1, DelayJobProfile("p", 100), 1)
        h._allocate(j1)
        h._allocate(j2)
        j1._submit(1)
        j2._submit(1)
        j1._allocate([h.id])
        j2._allocate([h.id])
        j1._start(1)
        j2._start(1)

        assert h.is_computing and h.jobs and len(h.jobs) == 2
        assert h.jobs[0].is_running and h.jobs[1].is_running

    def test_release_when_job_kill_valid(self):
        h = Host(0, "n")
        j = Job("n", "w", 1, DelayJobProfile("p", 100), 1)
        h._allocate(j)
        j._submit(1)
        j._allocate([h.id])
        j._start(1)
        j._kill(10)
        assert h.is_idle and h.state == HostState.IDLE and not h.jobs

    def test_release_when_job_terminate_valid(self):
        h = Host(0, "n")
        j = Job("n", "w", 1, DelayJobProfile("p", 100), 1)
        h._allocate(j)
        j._submit(1)
        j._allocate([h.id])
        j._start(1)
        j._terminate(10, JobState.COMPLETED_SUCCESSFULLY)
        assert h.is_idle and h.state == HostState.IDLE and not h.jobs

    def test_release_dispatch_event(self):
        def foo(sender):
            self.__called = True

        self.__called = False
        dispatcher.subscribe(foo, HostEvent.STATE_CHANGED)
        h = Host(0, "n")
        j = Job("n", "w", 1, DelayJobProfile("p", 100), 1)
        h._allocate(j)
        j._submit(1)
        j._allocate([h.id])
        j._start(1)
        j._terminate(10, JobState.COMPLETED_SUCCESSFULLY)
        assert self.__called

    def test_release_when_multiple_jobs_must_remain_computing(self):
        h = Host(0, "n")
        j = Job("n", "w", 1, DelayJobProfile("p", 100), 1)
        j1 = Job("n1", "w", 1, DelayJobProfile("p", 100), 1)
        j2 = Job("n2", "w", 1, DelayJobProfile("p", 100), 1)
        h._allocate(j1)
        h._allocate(j2)
        j1._submit(1)
        j2._submit(1)
        j1._allocate([h.id])
        j2._allocate([h.id])
        j1._start(1)
        j2._start(1)
        j1._kill(10)
        assert h.is_computing and h.state == HostState.COMPUTING
        assert h.jobs and len(h.jobs) == 1 and h.jobs[0] == j2


class TestPlatform:
    def test_valid_instance(self):
        hosts = [Host(0, "n"), Host(1, "n")]
        p = Platform(hosts)
        assert all(h.id == i for i, h in enumerate(p))

    def test_empty_hosts_must_raise(self):
        with pytest.raises(ValueError):
            Platform([])

    def test_invalid_host_id_must_raise(self):
        hosts = [Host(1, "n"), Host(2, "n")]
        with pytest.raises(SystemError):
            Platform(hosts)

    def test_size(self):
        hosts = [Host(0, "n"), Host(1, "n")]
        p = Platform(hosts)
        assert p.size == len(hosts)

    def test_power_when_pstates_not_defined(self):
        hosts = [Host(0, "n"), Host(1, "n")]
        p = Platform(hosts)
        assert p.power == 0

    def test_power_when_pstates_defined(self):
        p1 = PowerState(1, PowerStateType.COMPUTATION, 15, 100)
        p2 = PowerState(1, PowerStateType.COMPUTATION, 50, 100)
        hosts = [Host(0, "n", pstates=[p1]), Host(1, "n", pstates=[p2])]
        p = Platform(hosts)
        assert p.power == 15 + 50

    def test_state(self):
        p1 = PowerState(0, PowerStateType.COMPUTATION, 15, 100)
        p2 = PowerState(1, PowerStateType.SLEEP, 15, 15)
        p3 = PowerState(2, PowerStateType.SWITCHING_OFF, 15, 15)
        h1 = Host(0, "n", pstates=[p1, p2, p3])
        h2 = Host(1, "n")
        p = Platform([h1, h2])
        h1._switch_off()
        assert p.state == [HostState.SWITCHING_OFF, HostState.IDLE]

    def test_get_available(self):
        h1 = Host(0, "n")
        h2 = Host(1, "n")
        p = Platform([h1, h2])

        j1 = Job("n1", "w", 1, DelayJobProfile("p", 100), 1)
        h1._allocate(j1)

        assert p.get_available() == [h2]

    def test_get_by_role(self):
        h1 = Host(0, "n", role=HostRole.STORAGE)
        h2 = Host(1, "n")
        p = Platform([h1, h2])

        assert p.get_by_role(HostRole.COMPUTE) == [h2]

    def test_get_by_id(self):
        h1 = Host(0, "n", role=HostRole.STORAGE)
        h2 = Host(1, "n")
        p = Platform([h1, h2])

        assert p.get_by_id(1) == h2

    def test_get_by_id_not_found_must_raise(self):
        h1 = Host(0, "n", role=HostRole.STORAGE)
        h2 = Host(1, "n")
        p = Platform([h1, h2])
        with pytest.raises(LookupError):
            p.get_by_id(2)
