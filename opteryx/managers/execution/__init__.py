from opteryx.exceptions import InvalidInternalStateError

from .parallel_engine import execute as parallel_execute
from .serial_engine import execute as serial_execute


def execute(plan, statistics):
    # Validate query plan to ensure it's acyclic
    if not plan.is_acyclic():
        raise InvalidInternalStateError("Query plan is cyclic, cannot execute.")

    # Label the join legs to ensure left/right ordering
    plan.label_join_legs()

    """
    If we have 1 CPU, or less than 1Gb/CPU we use the serial engine.
    """

    # yield from parallel_execute(plan, statistics=statistics)
    yield from serial_execute(plan, statistics=statistics)
