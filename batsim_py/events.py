from enum import Enum


class JobEvent(Enum):
    """ Job Events """
    SUBMITTED = 0
    ALLOCATED = 1
    REJECTED = 2
    STARTED = 3
    COMPLETED = 4


class HostEvent(Enum):
    """ Host Events """
    STATE_CHANGED = 0
    COMPUTATION_POWER_STATE_CHANGED = 1


class SimulatorEvent(Enum):
    """ Simulator Events """
    SIMULATION_BEGINS = 0
    SIMULATION_ENDS = 1
