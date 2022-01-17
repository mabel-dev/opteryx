def summer(x, y):
    import decimal

    return decimal.Decimal(x) + decimal.Decimal(y)


def raise_not_implemented():
    raise NotImplementedError()


AGGREGATORS = {
    "SUM": summer,
    "MAX": max,
    "MIN": min,
    "COUNT": lambda x, y: x + 1,
    "AVG": lambda x, y: 1,
    "RANGE": raise_not_implemented,  # difference between min and max
    "STDDEV_POP": raise_not_implemented,
    "STDDEV": raise_not_implemented,
    "HISTOGRAM": raise_not_implemented,  # returns a histogram of the values (100 buckets)
    "PERCENT": raise_not_implemented,  # each group has the relative portion calculated
    "APPROX_MODE": raise_not_implemented,  # the mode of the values, use lossy counter
    "APPROX_MEDIAN": raise_not_implemented,  # the median of the values, use tdigest, 50% centile
    "APPROX_DISTINCT": raise_not_implemented,  # hyperloglog is used to estimate distinct values
    "APPROX_IQR": raise_not_implemented,  # use tdigest
}
