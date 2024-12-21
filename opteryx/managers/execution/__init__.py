from opteryx.exceptions import InvalidInternalStateError

from .serial_engine import execute as serial_execute


def execute(plan, statistics):
    # Validate query plan to ensure it's acyclic
    if not plan.is_acyclic():
        raise InvalidInternalStateError("Query plan is cyclic, cannot execute.")

    # Label the join legs to ensure left/right ordering
    plan.label_join_legs()

    yield from serial_execute(plan, statistics=statistics)
