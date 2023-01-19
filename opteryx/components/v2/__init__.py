from opteryx.components.v2.logical_planner.planner import QUERY_BUILDERS


def engine_version(query):
    statement_type = next(iter(query))
    if statement_type in QUERY_BUILDERS:
        return "v2"
    return "v1"
