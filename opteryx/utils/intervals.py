import numpy


# based on: https://stackoverflow.com/a/57321916
# license:  https://creativecommons.org/licenses/by-sa/4.0/
def generate_range(*args):
    """
    Combines numpy.arange and numpy.isclose to mimic
    open, half-open and closed intervals.
    Avoids also floating point rounding errors as with
    >>> numpy.arange(1, 1.3, 0.1)
    array([1. , 1.1, 1.2, 1.3])

    args: [start, ]stop, [step, ]
        as in numpy.arange
    rtol, atol: floats
        floating point tolerance as in numpy.isclose
    include: boolean list-like, length 2
        if start and end point are included
    """
    # process arguments
    if len(args) == 1:
        start = 0
        stop = args[0]
        step = 1
    elif len(args) == 2:
        start, stop = args
        step = 1
        stop += step
    else:
        assert len(args) == 3
        start, stop, step = tuple(args)

        # ensure the the last item is in the series
        if ((stop - start) / step) % 1 == 0:
            stop += step

    return numpy.arange(start, stop, step, dtype=numpy.float64)
