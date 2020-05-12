
from batsim_py.events import HostEvent, JobEvent, SimulatorEvent
from batsim_py.events import Event


def test_event_str():
    e = HostEvent.ALLOCATED
    assert str(e) == "ALLOCATED"


def test_event_repr():
    e = HostEvent.ALLOCATED
    assert repr(e) == "HostEvent.ALLOCATED"


def test_host_event():
    assert issubclass(HostEvent, Event)


def test_job_event():
    assert issubclass(JobEvent, Event)


def test_simulator_event():
    assert issubclass(SimulatorEvent, Event)
