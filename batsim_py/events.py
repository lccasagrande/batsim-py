from enum import Enum


class JobEvent(Enum):
    """ Job Event Types """
    SUBMITTED = 0
    ALLOCATED = 1
    REJECTED = 2
    STARTED = 3
    COMPLETED = 4


class HostEvent(Enum):
    """ Host Event Types """
    STATE_CHANGED = 0
    COMPUTATION_POWER_STATE_CHANGED = 1


class SimulatorEvent(Enum):
    """ Simulator Event Types """
    SIMULATION_BEGINS = 0
    SIMULATION_ENDS = 1
