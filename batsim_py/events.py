from enum import Enum


class Event(Enum):
    """ Event base class """

    def __str__(self) -> str:
        return "%s" % self.name

    def __repr__(self) -> str:
        return "%s.%s" % (self.__class__.__name__, self.name)


class HostEvent(Event):
    """ Host Event class

        This class enumerates the distinct events a host can dispatch.
    """
    STATE_CHANGED = 0
    COMPUTATION_POWER_STATE_CHANGED = 1
    ALLOCATED = 2


class JobEvent(Event):
    """ Job Event class

        This class enumerates the distinct events a job can dispatch.
    """
    SUBMITTED = 0
    COMPLETED = 1
    STARTED = 2
    REJECTED = 3
    ALLOCATED = 4


class SimulatorEvent(Event):
    """ Job Event class

        This class enumerates the distinct events a simulator can dispatch.
    """
    SIMULATION_BEGINS = 0
    SIMULATION_ENDS = 1
