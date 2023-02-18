from enum import Enum, auto


class ConsistencyMode(int, Enum):
    """
    Aggressive will ensure each write is flushed before moving to the next, Relaxed
    allows the OS to handle write flushes.

    Aggressive is at least have the speed, but significantly reduces the chance of
    data loss due to system crashes.

    Relaxed is faster, but if the system crashes, your data may be in an inconsistent
    state.

    Aggressive is the default (safe default), but there are some scenarios where a
    relaxed approach to write consistency is okay.
    """

    AGGRESSIVE = auto()
    RELAXED = auto()


WRITE_CONSISTENCY: ConsistencyMode = ConsistencyMode.AGGRESSIVE
