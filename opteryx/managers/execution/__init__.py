from opteryx.exceptions import InvalidInternalStateError
from opteryx.utils.threading import is_free_threading_available

from .parallel_engine import execute as parallel_execute
from .serial_engine import execute as serial_execute


def execute(plan, statistics):
    # Validate query plan to ensure it's acyclic
    if not plan.is_acyclic():
        raise InvalidInternalStateError("Query plan is cyclic, cannot execute.")

    # Label the join legs to ensure left/right ordering
    plan.label_join_legs()

    # Use parallel engine if free-threading is available, otherwise use serial
    if is_free_threading_available():
        yield from parallel_execute(plan, statistics=statistics)
    else:
        yield from serial_execute(plan, statistics=statistics)
