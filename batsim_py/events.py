from enum import Enum


class Event(Enum):
    def __str__(self):
        return "%s" % self.name

    def __repr__(self):
        return "%s.%s" % (self.__class__.__name__, self.name)


class HostEvent(Event):
    STATE_CHANGED = 0
    COMPUTATION_POWER_STATE_CHANGED = 1
    ALLOCATED = 2


class JobEvent(Event):
    SUBMITTED = 0
    COMPLETED = 1
    STARTED = 2
    REJECTED = 3
    KILLED = 4
    ALLOCATED = 5


class SimulatorEvent(Event):
    SIMULATION_BEGINS = 0
    SIMULATION_ENDS = 1
