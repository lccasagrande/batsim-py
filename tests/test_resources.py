import pytest

from batsim_py.resources import Host
from batsim_py.resources import Storage
from batsim_py.resources import HostState
from batsim_py.resources import Platform
from batsim_py.resources import PowerState
from batsim_py.resources import PowerStateType


class TestHostState:
    def test_str(self):
        for s in HostState:
            assert str(s) == s.name.upper()


class TestPowerStateType:
    def test_str(self):
        for s in PowerStateType:
            assert str(s) == s.name.upper()


class TestPowerState:
    def test_valid_instance(self):
        i, tp, idle, full = 0, PowerStateType.COMPUTATION, 100, 250
        p = PowerState(i, tp, idle, full)

        assert p.id == i and p.type == tp
        assert p.watt_idle == idle and p.watt_full == full

    def test_watt_idle_less_than_zero_must_raise(self):
        with pytest.raises(ValueError) as excinfo:
            PowerState(0, PowerStateType.COMPUTATION, -1, 10)

        assert "watt_idle" in str(excinfo.value)

    def test_watt_full_less_than_zero_must_raise(self):
        with pytest.raises(ValueError) as excinfo:
            PowerState(0, PowerStateType.COMPUTATION, 1, -1)

        assert "watt_full" in str(excinfo.value)

    def test_watts_can_be_zero(self):
        p = PowerState(0, PowerStateType.COMPUTATION, 0, 0)
        assert p.watt_full == 0 and p.watt_idle == 0

    def test_sleep_with_different_watts_must_raise(self):
        with pytest.raises(ValueError) as excinfo:
            PowerState(0, PowerStateType.SLEEP, 1, 2)

        assert "`watt_idle` and `watt_full" in str(excinfo.value)

    def test_switching_off_with_different_watts_must_raise(self):
        with pytest.raises(ValueError) as excinfo:
            PowerState(0, PowerStateType.SWITCHING_OFF, 1, 2)
        assert "`watt_idle` and `watt_full" in str(excinfo.value)

    def test_switching_on_with_different_watts_must_raise(self):
        with pytest.raises(ValueError) as excinfo:
            PowerState(0, PowerStateType.SWITCHING_ON, 1, 2)
        assert "`watt_idle` and `watt_full" in str(excinfo.value)


class TestHost:
    def test_valid_instance(self):
        p = PowerState(0, PowerStateType.COMPUTATION, 10, 10)
        i, n, r, m = 0, "n", [p], {'u': 1}
        h = Host(i, n, r, True, m)

        assert h.id == i and h.name == n
        assert h.pstate == p and h.metadata == m and not h.jobs

    def test_repr(self):
        assert repr(Host(0, "n")) == "Host_{}".format(0)

    def test_str(self):
        s = str(Host(0, "n"))
        assert s == "Host_{}: {}".format(0, str(HostState.IDLE))

    def test_initial_state_must_be_idle(self):
        p1 = PowerState(1, PowerStateType.COMPUTATION, 10, 100)
        h = Host(0, "n", pstates=[p1])
        assert h.state == HostState.IDLE and h.is_idle

    def test_pstates(self):
        p1 = PowerState(1, PowerStateType.COMPUTATION, 10, 100)
        h = Host(0, "n", pstates=[p1])
        assert h.pstates == [p1]

    def test_pstates_must_have_computation(self):
        p1 = PowerState(0, PowerStateType.SLEEP, 10, 10)
        p2 = PowerState(1, PowerStateType.SWITCHING_ON, 10, 10)
        p4 = PowerState(3, PowerStateType.SWITCHING_OFF, 50, 50)
        with pytest.raises(ValueError) as excinfo:
            Host(0, "n", pstates=[p1, p2, p4])
        assert "at least one COMPUTATION" in str(excinfo.value)

    def test_pstates_must_have_unique_id(self):
        p1 = PowerState(1, PowerStateType.COMPUTATION, 10, 10)
        p2 = PowerState(1, PowerStateType.COMPUTATION, 100, 100)
        with pytest.raises(ValueError) as excinfo:
            Host(0, "n", pstates=[p1, p2])

        assert "unique ids" in str(excinfo.value)

    def test_sleep_pstate_without_switching_on_must_raise(self):
        p1 = PowerState(0, PowerStateType.SLEEP, 10, 10)
        p2 = PowerState(1, PowerStateType.COMPUTATION, 10, 100)
        p4 = PowerState(3, PowerStateType.SWITCHING_OFF, 50, 50)
        with pytest.raises(ValueError) as excinfo:
            Host(0, "n", pstates=[p1, p2, p4])
        assert "switching on" in str(excinfo.value)

    def test_sleep_pstate_without_switching_off_must_raise(self):
        p1 = PowerState(0, PowerStateType.SLEEP, 10, 10)
        p2 = PowerState(1, PowerStateType.COMPUTATION, 10, 100)
        p4 = PowerState(3, PowerStateType.SWITCHING_ON, 50, 50)
        with pytest.raises(ValueError) as excinfo:
            Host(0, "n", pstates=[p1, p2, p4])
        assert "switching off" in str(excinfo.value)

    def test_transition_pstate_without_sleep_must_raise(self):
        p1 = PowerState(0, PowerStateType.SWITCHING_OFF, 10, 10)
        p2 = PowerState(1, PowerStateType.COMPUTATION, 10, 100)
        p4 = PowerState(3, PowerStateType.SWITCHING_ON, 50, 50)
        with pytest.raises(ValueError) as excinfo:
            Host(0, "n", pstates=[p1, p2, p4])
        assert "sleeping" in str(excinfo.value)

    def test_switching_off_pstate_must_be_unique(self):
        p1 = PowerState(0, PowerStateType.SLEEP, 10, 10)
        p2 = PowerState(1, PowerStateType.SWITCHING_OFF, 10, 10)
        p3 = PowerState(2, PowerStateType.COMPUTATION, 10, 100)
        p4 = PowerState(3, PowerStateType.SWITCHING_ON, 50, 50)
        p5 = PowerState(4, PowerStateType.SWITCHING_OFF, 10, 10)
        with pytest.raises(ValueError) as excinfo:
            Host(0, "n", pstates=[p1, p2, p3, p4, p5])
        assert "switching off" in str(excinfo.value)

    def test_switching_on_pstate_must_be_unique(self):
        p1 = PowerState(0, PowerStateType.SLEEP, 10, 10)
        p2 = PowerState(1, PowerStateType.SWITCHING_OFF, 10, 10)
        p3 = PowerState(2, PowerStateType.COMPUTATION, 10, 100)
        p4 = PowerState(3, PowerStateType.SWITCHING_ON, 50, 50)
        p5 = PowerState(4, PowerStateType.SWITCHING_ON, 10, 10)
        with pytest.raises(ValueError) as excinfo:
            Host(0, "n", pstates=[p1, p2, p3, p4, p5])
        assert "switching on" in str(excinfo.value)

    def test_sleep_pstate_must_be_unique(self):
        p1 = PowerState(0, PowerStateType.SLEEP, 10, 10)
        p2 = PowerState(1, PowerStateType.SWITCHING_OFF, 10, 10)
        p3 = PowerState(2, PowerStateType.COMPUTATION, 10, 100)
        p4 = PowerState(3, PowerStateType.SWITCHING_ON, 50, 50)
        p5 = PowerState(4, PowerStateType.SLEEP, 10, 10)
        with pytest.raises(ValueError) as excinfo:
            Host(0, "n", pstates=[p1, p2, p3, p4, p5])
        assert "sleeping" in str(excinfo.value)

    def test_initial_pstate_must_be_the_first_computation_one(self):
        p1 = PowerState(0, PowerStateType.SLEEP, 10, 10)
        p2 = PowerState(1, PowerStateType.COMPUTATION, 10, 100)
        p3 = PowerState(2, PowerStateType.COMPUTATION, 1000, 10000)
        p4 = PowerState(3, PowerStateType.SWITCHING_OFF, 50, 50)
        p5 = PowerState(4, PowerStateType.SWITCHING_ON, 25, 25)
        h = Host(0, "n", pstates=[p1, p3, p2, p4, p5])
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
        h._allocate("job")
        h._start_computing()
        assert h.power == 100 and h.is_computing

    def test_get_pstate_by_type_must_return_all_matches(self):
        p1 = PowerState(0, PowerStateType.SLEEP, 10, 10)
        p2 = PowerState(1, PowerStateType.COMPUTATION, 10, 100)
        p3 = PowerState(2, PowerStateType.COMPUTATION, 1000, 10000)
        p4 = PowerState(3, PowerStateType.SWITCHING_OFF, 50, 50)
        p5 = PowerState(4, PowerStateType.SWITCHING_ON, 25, 25)
        h = Host(0, "n", pstates=[p1, p3, p2, p4, p5])
        pstates = h.get_pstate_by_type(PowerStateType.COMPUTATION)
        assert pstates == [p2, p3]

    def test_get_pstate_by_type_not_found_must_raise(self):
        p2 = PowerState(1, PowerStateType.COMPUTATION, 10, 100)
        p3 = PowerState(2, PowerStateType.COMPUTATION, 1000, 10000)
        h = Host(0, "n", pstates=[p3, p2])
        with pytest.raises(LookupError) as excinfo:
            h.get_pstate_by_type(PowerStateType.SWITCHING_OFF)

        assert "not be found" in str(excinfo.value)

    def test_get_pstate_by_type_not_defined_must_raise(self):
        h = Host(0, "n", pstates=[])
        with pytest.raises(RuntimeError) as excinfo:
            h.get_pstate_by_type(PowerStateType.SWITCHING_OFF)

        assert "undefined" in str(excinfo.value)

    def test_get_pstate_by_id_valid(self):
        p2 = PowerState(1, PowerStateType.COMPUTATION, 10, 100)
        p3 = PowerState(2, PowerStateType.COMPUTATION, 1000, 10000)
        h = Host(0, "n", pstates=[p3, p2])
        assert h.get_pstate_by_id(1) == p2
        assert h.get_pstate_by_id(2) == p3

    def test_get_pstate_by_id_not_found_must_raise(self):
        p2 = PowerState(1, PowerStateType.COMPUTATION, 10, 100)
        p3 = PowerState(2, PowerStateType.COMPUTATION, 1000, 10000)
        h = Host(0, "n", pstates=[p3, p2])

        with pytest.raises(LookupError) as excinfo:
            h.get_pstate_by_id(4)

        assert "not be found" in str(excinfo.value)

    def test_get_pstate_by_id_not_defined_must_raise(self):
        h = Host(0, "n", pstates=[])
        with pytest.raises(RuntimeError) as excinfo:
            h.get_pstate_by_id(4)
        assert "undefined" in str(excinfo.value)

    def test_get_sleep_pstate_valid(self):
        p1 = PowerState(0, PowerStateType.SLEEP, 10, 10)
        p2 = PowerState(1, PowerStateType.COMPUTATION, 10, 100)
        p3 = PowerState(2, PowerStateType.COMPUTATION, 1000, 10000)
        p4 = PowerState(3, PowerStateType.SWITCHING_OFF, 50, 50)
        p5 = PowerState(4, PowerStateType.SWITCHING_ON, 25, 25)
        h = Host(0, "n", pstates=[p1, p3, p2, p4, p5])
        assert h.get_sleep_pstate() == p1

    def test_get_sleep_pstate_not_found_must_raise(self):
        p2 = PowerState(1, PowerStateType.COMPUTATION, 10, 100)
        h = Host(0, "n", pstates=[p2])
        with pytest.raises(LookupError) as excinfo:
            h.get_sleep_pstate()
        assert "not be found" in str(excinfo.value)

    def test_get_sleep_pstate_not_defined_must_raise(self):
        h = Host(0, "n", pstates=[])
        with pytest.raises(RuntimeError) as excinfo:
            h.get_sleep_pstate()
        assert "undefined" in str(excinfo.value)

    def test_get_default_pstate_valid(self):
        p1 = PowerState(0, PowerStateType.SLEEP, 10, 10)
        p2 = PowerState(1, PowerStateType.COMPUTATION, 10, 100)
        p3 = PowerState(2, PowerStateType.COMPUTATION, 1000, 10000)
        p4 = PowerState(3, PowerStateType.SWITCHING_OFF, 50, 50)
        p5 = PowerState(4, PowerStateType.SWITCHING_ON, 25, 25)
        h = Host(0, "n", pstates=[p1, p3, p2, p4, p5])
        assert h.get_default_pstate() == p2

    def test_get_default_pstate_not_defined_must_raise(self):
        h = Host(0, "n", pstates=[])
        with pytest.raises(RuntimeError) as excinfo:
            h.get_default_pstate()
        assert "undefined" in str(excinfo.value)

    def test_switch_off_valid(self):
        p1 = PowerState(0, PowerStateType.SLEEP, 10, 10)
        p2 = PowerState(1, PowerStateType.COMPUTATION, 10, 100)
        p3 = PowerState(2, PowerStateType.COMPUTATION, 1000, 10000)
        p4 = PowerState(3, PowerStateType.SWITCHING_OFF, 50, 50)
        p5 = PowerState(4, PowerStateType.SWITCHING_ON, 25, 25)
        h = Host(0, "n", pstates=[p1, p3, p2, p4, p5])

        h._switch_off()
        assert h.is_switching_off and h.power == p4.watt_idle
        assert h.pstate == p4 and h.state == HostState.SWITCHING_OFF

    def test_switch_off_without_pstates_defined_must_raise(self):
        h = Host(0, "n", pstates=[])

        with pytest.raises(RuntimeError) as excinfo:
            h._switch_off()
        assert "undefined" in str(excinfo.value)

    def test_switch_off_already_switching_off_must_raises(self):
        p1 = PowerState(0, PowerStateType.SLEEP, 10, 10)
        p2 = PowerState(1, PowerStateType.COMPUTATION, 10, 100)
        p3 = PowerState(2, PowerStateType.COMPUTATION, 1000, 10000)
        p4 = PowerState(3, PowerStateType.SWITCHING_OFF, 50, 50)
        p5 = PowerState(4, PowerStateType.SWITCHING_ON, 25, 25)
        h = Host(0, "n", pstates=[p1, p3, p2, p4, p5])
        h._switch_off()
        with pytest.raises(RuntimeError) as excinfo:
            h._switch_off()
        assert "idle and free " in str(excinfo.value)

    def test_switch_off_when_off_must_raises(self):
        p1 = PowerState(0, PowerStateType.SLEEP, 10, 10)
        p2 = PowerState(1, PowerStateType.COMPUTATION, 10, 100)
        p3 = PowerState(2, PowerStateType.COMPUTATION, 1000, 10000)
        p4 = PowerState(3, PowerStateType.SWITCHING_OFF, 50, 50)
        p5 = PowerState(4, PowerStateType.SWITCHING_ON, 25, 25)
        h = Host(0, "n", pstates=[p1, p3, p2, p4, p5])

        h._switch_off()
        h._set_off()
        with pytest.raises(RuntimeError) as excinfo:
            h._switch_off()
        assert "idle and free " in str(excinfo.value)

    def test_switch_off_switching_on_must_raises(self):
        p1 = PowerState(0, PowerStateType.SLEEP, 10, 10)
        p2 = PowerState(1, PowerStateType.COMPUTATION, 10, 100)
        p3 = PowerState(2, PowerStateType.COMPUTATION, 1000, 10000)
        p4 = PowerState(3, PowerStateType.SWITCHING_OFF, 50, 50)
        p5 = PowerState(4, PowerStateType.SWITCHING_ON, 25, 25)
        h = Host(0, "n", pstates=[p1, p3, p2, p4, p5])

        h._switch_off()
        h._set_off()
        h._switch_on()
        with pytest.raises(RuntimeError) as excinfo:
            h._switch_off()
        assert "idle and free " in str(excinfo.value)

    def test_switch_off_computing_must_raise(self):
        p1 = PowerState(0, PowerStateType.SLEEP, 10, 10)
        p2 = PowerState(1, PowerStateType.COMPUTATION, 10, 100)
        p3 = PowerState(2, PowerStateType.COMPUTATION, 1000, 10000)
        p4 = PowerState(3, PowerStateType.SWITCHING_OFF, 50, 50)
        p5 = PowerState(4, PowerStateType.SWITCHING_ON, 25, 25)
        h = Host(0, "n", pstates=[p1, p3, p2, p4, p5])
        h._allocate("n")
        h._start_computing()

        with pytest.raises(RuntimeError) as excinfo:
            h._switch_off()
        assert "idle and free " in str(excinfo.value)

    def test_switch_off_allocated_must_raise(self):
        p1 = PowerState(0, PowerStateType.SLEEP, 10, 10)
        p2 = PowerState(1, PowerStateType.COMPUTATION, 10, 100)
        p3 = PowerState(2, PowerStateType.COMPUTATION, 1000, 10000)
        p4 = PowerState(3, PowerStateType.SWITCHING_OFF, 50, 50)
        p5 = PowerState(4, PowerStateType.SWITCHING_ON, 25, 25)
        h = Host(0, "n", pstates=[p1, p3, p2, p4, p5])
        h._allocate("job")
        with pytest.raises(RuntimeError) as excinfo:
            h._switch_off()
        assert "idle and free " in str(excinfo.value)

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

    def test_switch_on_already_switching_on_must_raises(self):
        p1 = PowerState(0, PowerStateType.SLEEP, 10, 10)
        p2 = PowerState(1, PowerStateType.COMPUTATION, 10, 100)
        p3 = PowerState(2, PowerStateType.SWITCHING_OFF, 50, 50)
        p4 = PowerState(3, PowerStateType.SWITCHING_ON, 75, 75)
        h = Host(0, "n", pstates=[p1, p3, p2, p4])

        h._switch_off()
        h._set_off()
        h._switch_on()
        with pytest.raises(RuntimeError) as excinfo:
            h._switch_on()
        assert "be sleeping" in str(excinfo.value)

    def test_switch_on_switching_off_must_raises(self):
        p1 = PowerState(0, PowerStateType.SLEEP, 10, 10)
        p2 = PowerState(1, PowerStateType.COMPUTATION, 10, 100)
        p3 = PowerState(2, PowerStateType.SWITCHING_OFF, 50, 50)
        p4 = PowerState(3, PowerStateType.SWITCHING_ON, 75, 75)
        h = Host(0, "n", pstates=[p1, p3, p2, p4])

        h._switch_off()
        with pytest.raises(RuntimeError) as excinfo:
            h._switch_on()
        assert "be sleeping" in str(excinfo.value)

    def test_switch_on_idle_must_raises(self):
        p1 = PowerState(0, PowerStateType.SLEEP, 10, 10)
        p2 = PowerState(1, PowerStateType.COMPUTATION, 10, 100)
        p3 = PowerState(2, PowerStateType.SWITCHING_OFF, 50, 50)
        p4 = PowerState(3, PowerStateType.SWITCHING_ON, 75, 75)
        h = Host(0, "n", pstates=[p1, p3, p2, p4])
        with pytest.raises(RuntimeError) as excinfo:
            h._switch_on()
        assert "be sleeping" in str(excinfo.value)

    def test_switch_on_computing_must_raise(self):
        p1 = PowerState(0, PowerStateType.SLEEP, 10, 10)
        p2 = PowerState(1, PowerStateType.COMPUTATION, 10, 100)
        p3 = PowerState(2, PowerStateType.SWITCHING_OFF, 50, 50)
        p4 = PowerState(3, PowerStateType.SWITCHING_ON, 75, 75)
        h = Host(0, "n", pstates=[p1, p3, p2, p4])

        h._allocate("job")
        h._start_computing()

        with pytest.raises(RuntimeError) as excinfo:
            h._switch_on()
        assert "be sleeping" in str(excinfo.value)

    def test_switch_on_allocated_must_raise(self):
        p1 = PowerState(0, PowerStateType.SLEEP, 10, 10)
        p2 = PowerState(1, PowerStateType.COMPUTATION, 10, 100)
        p3 = PowerState(2, PowerStateType.SWITCHING_OFF, 50, 50)
        p4 = PowerState(3, PowerStateType.SWITCHING_ON, 75, 75)
        h = Host(0, "n", pstates=[p1, p3, p2, p4])

        h._allocate("job")

        with pytest.raises(RuntimeError) as excinfo:
            h._switch_on()
        assert "be sleeping" in str(excinfo.value)

    def test_set_computation_state_valid(self):
        p1 = PowerState(0, PowerStateType.COMPUTATION, 10, 100)
        p2 = PowerState(1, PowerStateType.COMPUTATION, 15, 150)
        h = Host(0, "n", pstates=[p1, p2])
        h._set_computation_pstate(1)

        assert h.pstate == p2

    def test_set_computation_state_not_found_must_raise(self):
        p1 = PowerState(0, PowerStateType.COMPUTATION, 10, 100)
        p2 = PowerState(1, PowerStateType.COMPUTATION, 15, 150)
        h = Host(0, "n", pstates=[p1, p2])
        with pytest.raises(LookupError) as excinfo:
            h._set_computation_pstate(2)
        assert "not be found" in str(excinfo.value)

    def test_set_computation_state_not_defined_must_raise(self):
        h = Host(0, "n", pstates=[])
        with pytest.raises(RuntimeError) as excinfo:
            h._set_computation_pstate(2)
        assert "undefined" in str(excinfo.value)

    def test_set_computation_state_not_correct_type_must_raise(self):
        p1 = PowerState(0, PowerStateType.SLEEP, 10, 10)
        p2 = PowerState(1, PowerStateType.COMPUTATION, 10, 100)
        p3 = PowerState(2, PowerStateType.COMPUTATION, 15, 150)
        p4 = PowerState(3, PowerStateType.SWITCHING_OFF, 50, 50)
        p5 = PowerState(4, PowerStateType.SWITCHING_ON, 75, 75)
        h = Host(0, "n", pstates=[p1, p3, p2, p4, p5])
        with pytest.raises(RuntimeError) as excinfo:
            h._set_computation_pstate(0)
        assert "computation" in str(excinfo.value)

    def test_set_computation_state_when_switching_off_must_raise(self):
        p1 = PowerState(0, PowerStateType.SLEEP, 10, 10)
        p2 = PowerState(1, PowerStateType.COMPUTATION, 10, 100)
        p3 = PowerState(2, PowerStateType.COMPUTATION, 15, 150)
        p4 = PowerState(3, PowerStateType.SWITCHING_OFF, 50, 50)
        p5 = PowerState(4, PowerStateType.SWITCHING_ON, 75, 75)
        h = Host(0, "n", pstates=[p1, p3, p2, p4, p5])
        h._switch_off()
        with pytest.raises(RuntimeError) as excinfo:
            h._set_computation_pstate(2)
        assert "idle or computing" in str(excinfo.value)

    def test_set_computation_state_when_off_must_raise(self):
        p1 = PowerState(0, PowerStateType.SLEEP, 10, 10)
        p2 = PowerState(1, PowerStateType.COMPUTATION, 10, 100)
        p3 = PowerState(2, PowerStateType.COMPUTATION, 15, 150)
        p4 = PowerState(3, PowerStateType.SWITCHING_OFF, 50, 50)
        p5 = PowerState(4, PowerStateType.SWITCHING_ON, 75, 75)
        h = Host(0, "n", pstates=[p1, p3, p2, p4, p5])
        h._switch_off()
        h._set_off()
        with pytest.raises(RuntimeError) as excinfo:
            h._set_computation_pstate(2)
        assert "idle or computing" in str(excinfo.value)

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
        with pytest.raises(RuntimeError) as excinfo:
            h._set_computation_pstate(2)
        assert "idle or computing" in str(excinfo.value)

    def test_set_computation_state_when_computing_valid(self):
        p1 = PowerState(0, PowerStateType.SLEEP, 10, 10)
        p2 = PowerState(1, PowerStateType.COMPUTATION, 10, 100)
        p3 = PowerState(2, PowerStateType.COMPUTATION, 15, 150)
        p4 = PowerState(3, PowerStateType.SWITCHING_OFF, 50, 50)
        p5 = PowerState(4, PowerStateType.SWITCHING_ON, 75, 75)
        h = Host(0, "n", pstates=[p1, p3, p2, p4, p5])

        h._allocate("job")
        h._start_computing()
        h._set_computation_pstate(2)
        assert h.pstate == p3 and h.power == p3.watt_full

    def test_set_off_valid(self):
        p1 = PowerState(0, PowerStateType.SLEEP, 10, 10)
        p2 = PowerState(1, PowerStateType.COMPUTATION, 10, 100)
        p3 = PowerState(3, PowerStateType.SWITCHING_OFF, 50, 50)
        p4 = PowerState(4, PowerStateType.SWITCHING_ON, 75, 75)
        h = Host(0, "n", pstates=[p1, p3, p2, p4])
        h._switch_off()
        h._set_off()

        assert h.is_sleeping and h.power == p1.watt_full
        assert h.state == HostState.SLEEPING and h.pstate == p1

    def test_set_off_when_idle_must_raise(self):
        p1 = PowerState(0, PowerStateType.SLEEP, 10, 10)
        p2 = PowerState(1, PowerStateType.COMPUTATION, 10, 100)
        p3 = PowerState(3, PowerStateType.SWITCHING_OFF, 50, 50)
        p4 = PowerState(4, PowerStateType.SWITCHING_ON, 75, 75)
        h = Host(0, "n", pstates=[p1, p3, p2, p4])
        with pytest.raises(SystemError) as excinfo:
            h._set_off()
        assert "switching off" in str(excinfo.value)

    def test_set_off_when_computing_must_raise(self):
        p1 = PowerState(0, PowerStateType.SLEEP, 10, 10)
        p2 = PowerState(1, PowerStateType.COMPUTATION, 10, 100)
        p3 = PowerState(3, PowerStateType.SWITCHING_OFF, 50, 50)
        p4 = PowerState(4, PowerStateType.SWITCHING_ON, 75, 75)
        h = Host(0, "n", pstates=[p1, p3, p2, p4])
        h._allocate("job")
        h._start_computing()
        with pytest.raises(SystemError) as excinfo:
            h._set_off()
        assert "switching off" in str(excinfo.value)

    def test_set_off_when_switching_on_must_raise(self):
        p1 = PowerState(0, PowerStateType.SLEEP, 10, 10)
        p2 = PowerState(1, PowerStateType.COMPUTATION, 10, 100)
        p3 = PowerState(3, PowerStateType.SWITCHING_OFF, 50, 50)
        p4 = PowerState(4, PowerStateType.SWITCHING_ON, 75, 75)
        h = Host(0, "n", pstates=[p1, p3, p2, p4])
        h._switch_off()
        h._set_off()
        h._switch_on()
        with pytest.raises(SystemError) as excinfo:
            h._set_off()
        assert "switching off" in str(excinfo.value)

    def test_set_off_when_off_must_raise(self):
        p1 = PowerState(0, PowerStateType.SLEEP, 10, 10)
        p2 = PowerState(1, PowerStateType.COMPUTATION, 10, 100)
        p3 = PowerState(3, PowerStateType.SWITCHING_OFF, 50, 50)
        p4 = PowerState(4, PowerStateType.SWITCHING_ON, 75, 75)
        h = Host(0, "n", pstates=[p1, p3, p2, p4])
        h._switch_off()
        h._set_off()
        with pytest.raises(SystemError) as excinfo:
            h._set_off()
        assert "switching off" in str(excinfo.value)

    def test_set_on_valid(self):
        p1 = PowerState(0, PowerStateType.SLEEP, 10, 10)
        p2 = PowerState(1, PowerStateType.COMPUTATION, 10, 100)
        p3 = PowerState(3, PowerStateType.SWITCHING_OFF, 50, 50)
        p4 = PowerState(4, PowerStateType.SWITCHING_ON, 75, 75)
        h = Host(0, "n", pstates=[p1, p3, p2, p4])
        h._switch_off()
        h._set_off()
        h._switch_on()
        h._set_on()

        assert h.is_idle and h.pstate == p2 and h.state == HostState.IDLE

    def test_set_on_when_computing_must_raise(self):
        p1 = PowerState(0, PowerStateType.SLEEP, 10, 10)
        p2 = PowerState(1, PowerStateType.COMPUTATION, 10, 100)
        p3 = PowerState(3, PowerStateType.SWITCHING_OFF, 50, 50)
        p4 = PowerState(4, PowerStateType.SWITCHING_ON, 75, 75)
        h = Host(0, "n", pstates=[p1, p3, p2, p4])
        h._allocate("job")
        h._start_computing()
        with pytest.raises(SystemError) as excinfo:
            h._set_on()
        assert "switching on" in str(excinfo.value)

    def test_set_on_when_idle_must_raise(self):
        p1 = PowerState(0, PowerStateType.SLEEP, 10, 10)
        p2 = PowerState(1, PowerStateType.COMPUTATION, 10, 100)
        p3 = PowerState(2, PowerStateType.SWITCHING_OFF, 50, 50)
        p4 = PowerState(3, PowerStateType.SWITCHING_ON, 75, 75)
        h = Host(0, "n", pstates=[p1, p3, p2, p4])
        with pytest.raises(SystemError) as excinfo:
            h._set_on()
        assert "switching on" in str(excinfo.value)

    def test_set_on_when_switching_off_must_raise(self):
        p1 = PowerState(0, PowerStateType.SLEEP, 10, 10)
        p2 = PowerState(1, PowerStateType.COMPUTATION, 10, 100)
        p3 = PowerState(2, PowerStateType.SWITCHING_OFF, 50, 50)
        p4 = PowerState(3, PowerStateType.SWITCHING_ON, 75, 75)
        h = Host(0, "n", pstates=[p1, p3, p2, p4])
        h._switch_off()
        with pytest.raises(SystemError) as excinfo:
            h._set_on()
        assert "switching on" in str(excinfo.value)

    def test_set_on_when_off_must_raise(self):
        p1 = PowerState(0, PowerStateType.SLEEP, 10, 10)
        p2 = PowerState(1, PowerStateType.COMPUTATION, 10, 100)
        p3 = PowerState(2, PowerStateType.SWITCHING_OFF, 50, 50)
        p4 = PowerState(3, PowerStateType.SWITCHING_ON, 75, 75)
        h = Host(0, "n", pstates=[p1, p3, p2, p4])
        h._switch_off()
        h._set_off()
        with pytest.raises(SystemError) as excinfo:
            h._set_on()
        assert "switching on" in str(excinfo.value)

    def test_is_unavailable(self):
        h = Host(0, "n")
        h._set_unavailable()
        assert h.is_unavailable

    def test_set_available(self):
        h = Host(0, "n")
        h._set_unavailable()
        h._set_available()
        assert not h.is_unavailable

    def test_set_available_without_being_unavailable_must_raise(self):
        h = Host(0, "n")
        with pytest.raises(SystemError) as excinfo:
            h._set_available()
        assert "unavailable" in str(excinfo.value)

    def test_set_unavailable_when_already_allocated_must_not_raise(self):
        h = Host(0, "n")
        h._allocate("job")
        h._set_unavailable()
        assert h.is_unavailable and h.jobs

    def test_set_unavailable_must_return_to_last_state(self):
        h = Host(0, "n")
        h._allocate("job")
        h._start_computing()
        h._set_unavailable()
        assert h.is_unavailable and not h.is_computing
        h._set_available()
        assert h.is_computing and not h.is_unavailable

    def test_allocate_valid(self):
        p1 = PowerState(1, PowerStateType.COMPUTATION, 10, 100)
        h = Host(0, "n", pstates=[p1])
        h._allocate("job")
        assert h.is_idle and h.jobs and h.jobs[0] == "job"

    def test_allocate_when_unavailable_must_raise(self):
        h = Host(0, "n")
        h._set_unavailable()
        with pytest.raises(RuntimeError) as excinfo:
            h._allocate("job")

        assert "unavailable" in str(excinfo.value)

    def test_allocate_multiple_jobs_valid(self):
        p1 = PowerState(1, PowerStateType.COMPUTATION, 10, 100)
        h = Host(0, "n", pstates=[p1], allow_sharing=True)
        h._allocate("j1")
        h._allocate("j2")
        assert h.is_idle and h.jobs and len(h.jobs) == 2
        assert "j1" in h.jobs
        assert "j2" in h.jobs

    def test_allocate_multiple_jobs_when_not_shareable_must_raise(self):
        p1 = PowerState(1, PowerStateType.COMPUTATION, 10, 100)
        h = Host(0, "n", pstates=[p1], allow_sharing=False)
        h._allocate("j1")
        with pytest.raises(RuntimeError) as excinfo:
            h._allocate("j2")
        assert "multiple jobs" in str(excinfo.value)

    def test_allocate_job_twice_must_not_raise(self):
        h = Host(0, "n", allow_sharing=True)
        h._allocate("j1")
        h._allocate("j1")
        assert h.jobs and len(h.jobs) == 1

    def test_start_computing_valid(self):
        p1 = PowerState(1, PowerStateType.COMPUTATION, 10, 100)
        h = Host(0, "n", pstates=[p1])
        h._allocate("job")
        h._start_computing()
        assert h.is_computing and h.state == HostState.COMPUTING

    def test_start_computing_when_unavailable_must_raise(self):
        p1 = PowerState(1, PowerStateType.COMPUTATION, 10, 100)
        h = Host(0, "n", pstates=[p1])
        h._allocate("job")
        h._set_unavailable()
        with pytest.raises(SystemError) as excinfo:
            h._start_computing()
        assert "Unavailable" in str(excinfo.value)

    def test_start_computing_when_off_must_raise(self):
        p1 = PowerState(0, PowerStateType.SLEEP, 10, 10)
        p2 = PowerState(1, PowerStateType.COMPUTATION, 10, 100)
        p3 = PowerState(2, PowerStateType.SWITCHING_OFF, 50, 50)
        p4 = PowerState(3, PowerStateType.SWITCHING_ON, 75, 75)
        h = Host(0, "n", pstates=[p1, p3, p2, p4])
        h._switch_off()
        h._set_off()
        h._allocate("job")
        with pytest.raises(SystemError) as excinfo:
            h._start_computing()
        assert "idle or computing" in str(excinfo.value)

    def test_start_computing_when_switching_off_must_raise(self):
        p1 = PowerState(0, PowerStateType.SLEEP, 10, 10)
        p2 = PowerState(1, PowerStateType.COMPUTATION, 10, 100)
        p3 = PowerState(2, PowerStateType.SWITCHING_OFF, 50, 50)
        p4 = PowerState(3, PowerStateType.SWITCHING_ON, 75, 75)
        h = Host(0, "n", pstates=[p1, p3, p2, p4])
        h._switch_off()
        h._allocate("job")

        with pytest.raises(SystemError) as excinfo:
            h._start_computing()
        assert "idle or computing" in str(excinfo.value)

    def test_start_computing_when_switching_on_must_raise(self):
        p1 = PowerState(0, PowerStateType.SLEEP, 10, 10)
        p2 = PowerState(1, PowerStateType.COMPUTATION, 10, 100)
        p3 = PowerState(2, PowerStateType.SWITCHING_OFF, 50, 50)
        p4 = PowerState(3, PowerStateType.SWITCHING_ON, 75, 75)
        h = Host(0, "n", pstates=[p1, p3, p2, p4])
        h._switch_off()
        h._set_off()
        h._switch_on()
        h._allocate("job")
        with pytest.raises(SystemError) as excinfo:
            h._start_computing()
        assert "idle or computing" in str(excinfo.value)

    def test_start_computing_without_job_must_raise(self):
        h = Host(0, "n")
        with pytest.raises(SystemError) as excinfo:
            h._start_computing()
        assert "allocated" in str(excinfo.value)

    def test_release_valid(self):
        h = Host(0, "n")
        h._allocate("job")
        h._start_computing()
        h._release("job")
        assert h.is_idle and h.state == HostState.IDLE and not h.jobs

    def test_release_when_multiple_jobs_must_remain_computing(self):
        h = Host(0, "n", allow_sharing=True)
        h._allocate("j1")
        h._allocate("j2")
        h._start_computing()
        h._release("j1")
        assert h.is_computing and h.state == HostState.COMPUTING
        assert h.jobs and len(h.jobs) == 1 and h.jobs[0] == "j2"


class TestStorage:
    def test_repr(self):
        assert repr(Storage(0, "0", True)) == f"Storage_{0}"

    def test_str(self):
        s = str(Storage(0, "0", True))
        assert s == f"Storage_{0}"

    def test_valid(self):
        m = {"n": 0}
        s = Storage(0, "0", True, m)
        assert s.id == 0
        assert s.name == "0"
        assert s.metadata == m
        assert not s.is_unavailable

    def test_metadata_optional(self):
        m = {"n": 0}
        s = Storage(0, "0", True)
        assert s.metadata == None

    def test_is_unavailable(self):
        m = {"n": 0}
        s = Storage(0, "0", True, m)
        s._set_unavailable()
        assert s.is_unavailable

    def test_set_available(self):
        s = Storage(0, "0")
        s._set_unavailable()
        s._set_available()
        assert not s.is_unavailable

    def test_set_available_without_being_unavailable_must_raise(self):
        m = {"n": 0}
        s = Storage(0, "0", True, m)
        with pytest.raises(SystemError) as excinfo:
            s._set_available()
        assert "unavailable" in str(excinfo.value)

    def test_set_unavailable_when_already_allocated_must_not_raise(self):
        m = {"n": 0}
        s = Storage(0, "0", True, m)
        s._allocate("job")
        s._set_unavailable()
        assert s.is_unavailable and s.jobs

    def test_allocate_valid(self):
        s = Storage(0, "n")
        s._allocate("job")
        assert s.jobs and s.jobs[0] == "job"

    def test_allocate_when_unavailable_must_raise(self):
        m = {"n": 0}
        s = Storage(0, "0", True, m)
        s._set_unavailable()
        with pytest.raises(RuntimeError) as excinfo:
            s._allocate("job")

        assert "unavailable" in str(excinfo.value)

    def test_allocate_multiple_jobs_valid(self):
        s = Storage(0, "n")
        s._allocate("j1")
        s._allocate("j2")
        assert "j1" in s.jobs
        assert "j2" in s.jobs

    def test_allocate_multiple_jobs_when_not_shareable_must_raise(self):
        s = Storage(0, "n", False)
        s._allocate("j1")
        with pytest.raises(RuntimeError) as excinfo:
            s._allocate("j2")
        assert "multiple jobs" in str(excinfo.value)

    def test_allocate_job_twice_must_not_raise(self):
        s = Storage(0, "n")
        s._allocate("j1")
        s._allocate("j1")
        assert s.jobs and len(s.jobs) == 1

    def test_release_valid(self):
        s = Storage(0, "n")
        s._allocate("job")
        s._release("job")
        assert not s.jobs

    def test_release_when_multiple_jobs_are_allocated(self):
        s = Storage(0, "n")
        s._allocate("j1")
        s._allocate("j2")
        s._release("j1")
        assert s.jobs and len(s.jobs) == 1 and s.jobs[0] == "j2"


class TestPlatform:
    def test_valid_instance(self):
        res = [Host(0, "n"), Host(1, "n"), Storage(2, "n")]
        p = Platform(res)
        assert all(h.id == i for i, h in enumerate(p.resources))

    def test_empty_res_must_raise(self):
        with pytest.raises(ValueError) as excinfo:
            Platform([])
        assert "one resource" in str(excinfo.value)

    def test_invalid_res_id_must_raise(self):
        with pytest.raises(SystemError) as excinfo:
            Platform([Host(1, "n"), Host(2, "n")])
        assert "resources id" in str(excinfo.value)

    def test_size(self):
        res = [Host(0, "n"), Host(1, "n"), Storage(2, "n")]
        p = Platform(res)
        assert p.size == len(res)

    def test_hosts(self):
        res = [Host(0, "n"), Host(1, "n"), Storage(2, "n")]
        p = Platform(res)
        assert len(list(p.hosts)) == 2

    def test_storages(self):
        res = [Host(0, "n"), Host(1, "n"), Storage(2, "n")]
        p = Platform(res)
        assert len(list(p.storages)) == 1

    def test_power_when_pstates_not_defined(self):
        res = [Host(0, "n"), Host(1, "n")]
        p = Platform(res)
        assert p.power == 0

    def test_power_when_pstates_defined(self):
        p1 = PowerState(1, PowerStateType.COMPUTATION, 15, 100)
        p2 = PowerState(1, PowerStateType.COMPUTATION, 50, 100)
        res = [Host(0, "n", pstates=[p1]), Host(1, "n", pstates=[p2])]
        p = Platform(res)
        assert p.power == 15 + 50

    def test_state(self):
        p1 = PowerState(0, PowerStateType.SLEEP, 10, 10)
        p2 = PowerState(1, PowerStateType.COMPUTATION, 10, 100)
        p3 = PowerState(2, PowerStateType.COMPUTATION, 15, 150)
        p4 = PowerState(3, PowerStateType.SWITCHING_OFF, 50, 50)
        p5 = PowerState(4, PowerStateType.SWITCHING_ON, 75, 75)
        h1 = Host(0, "n", pstates=[p1, p3, p2, p4, p5])
        h2 = Host(1, "n")
        p = Platform([h1, h2])
        h1._switch_off()
        assert p.state == [HostState.SWITCHING_OFF, HostState.IDLE]

    def test_get_not_allocated_hosts(self):
        h1 = Host(0, "n")
        h2 = Host(1, "n")
        p = Platform([h1, h2])
        h1._allocate("j1")

        assert p.get_not_allocated_hosts() == [h2]

    def test_get_with_host(self):
        h1 = Host(0, "n")
        s1 = Storage(1, "n")
        p = Platform([h1, s1])

        assert p.get(0) == h1

    def test_get_with_storage(self):
        h1 = Host(0, "n")
        s1 = Storage(1, "n")
        p = Platform([h1, s1])

        assert p.get(1) == s1

    def test_get_not_found_must_raise(self):
        h1 = Host(0, "n")
        s1 = Storage(1, "n")
        p = Platform([h1, s1])
        with pytest.raises(LookupError) as excinfo:
            p.get(2)

        assert "no resources" in str(excinfo.value)

    def test_get_host_by_id(self):
        h1 = Host(0, "n")
        s1 = Storage(1, "n")
        p = Platform([h1, s1])

        assert p.get_host(0) == h1

    def test_get_storage_by_id(self):
        h1 = Host(0, "n")
        s1 = Storage(1, "n")
        p = Platform([h1, s1])

        assert p.get_storage(1) == s1

    def test_get_storage_with_host_id_must_raise(self):
        h1 = Host(0, "n")
        s1 = Storage(1, "n")
        p = Platform([h1, s1])

        with pytest.raises(LookupError) as excinfo:
            p.get_storage(0)

        assert "storage" in str(excinfo.value)

    def test_get_storage_not_found_must_raise(self):
        h1 = Host(0, "n")
        s1 = Storage(1, "n")
        p = Platform([h1, s1])

        with pytest.raises(LookupError) as excinfo:
            p.get_storage(2)

        assert "resources" in str(excinfo.value)

    def test_get_host_with_storage_id_must_raise(self):
        h1 = Host(0, "n")
        s1 = Storage(1, "n")
        p = Platform([h1, s1])

        with pytest.raises(LookupError) as excinfo:
            p.get_host(1)

        assert "host" in str(excinfo.value)

    def test_get_host_not_found_must_raise(self):
        h1 = Host(0, "n")
        s1 = Storage(1, "n")
        p = Platform([h1, s1])

        with pytest.raises(LookupError) as excinfo:
            p.get_host(2)

        assert "resources" in str(excinfo.value)
