from opteryx.config import features
from opteryx.exceptions import InvalidInternalStateError
from opteryx.utils.threading import is_free_threading_available

from .parallel_engine import execute as parallel_execute
from .serial_engine import execute as serial_execute

ENABLE_FREE_THREADING = features.enable_free_threading


def execute(plan, statistics):
    # Validate query plan to ensure it's acyclic
    if not plan.is_acyclic():
        raise InvalidInternalStateError("Query plan is cyclic, cannot execute.")

    # Label the join legs to ensure left/right ordering
    plan.label_join_legs()

    # Use parallel engine if free-threading is available, otherwise use serial
    if ENABLE_FREE_THREADING and is_free_threading_available():
        # DEBUG: print("\033[38;2;255;184;108mUsing parallel execution engine.\033[0m")
        yield from parallel_execute(plan, statistics=statistics)
    else:
        yield from serial_execute(plan, statistics=statistics)
