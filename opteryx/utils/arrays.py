import numpy

from opteryx.exceptions import SqlError
from opteryx.utils import intervals, dates


def generate_series(*args):

    from opteryx.managers.expression import NodeType

    arg_len = len(args)
    arg_vals = [i.value for i in args]
    first_arg_type = args[0].token_type

    # if the parameters are numbers, generate series is an alias for range
    if first_arg_type in (NodeType.LITERAL_NUMERIC, numpy.float64):
        if arg_len not in (1, 2, 3):
            raise SqlError("generate_series for numbers takes 1,2 or 3 parameters.")
        return intervals.generate_range(*arg_vals)

    # if the params are timestamps, we create time intervals
    if first_arg_type == NodeType.LITERAL_TIMESTAMP:
        if arg_len != 3:
            raise SqlError(
                "generate_series for dates needs start, end, and interval parameters"
            )
        return dates.date_range(*arg_vals)

    # if the param is a CIDR, we create network ranges
    if first_arg_type == NodeType.LITERAL_VARCHAR:
        if arg_len not in (1,):
            raise SqlError("generate_series for strings takes 1 CIDR parameter.")

        import ipaddress

        ips = ipaddress.ip_network(arg_vals[0], strict=False)
        return [str(ip) for ip in ips]
