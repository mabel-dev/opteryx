# type:ignore
import math
from bisect import bisect_left
from functools import reduce
from itertools import accumulate
from operator import itemgetter
from typing import List
from typing import Optional
from typing import Tuple

import numpy

__author__ = """Romain Picard"""
__email__ = "romain.picard@oakbits.com"
__version__ = "3.0.0"

"""
The following changes have been made for Opteryx:
- The ability to weight the differences has been removed
- Dump and Load functionality
- Bulk load functionality added
"""


EPSILON = 1e-5
BIN_COUNT: int = 50
Bin = Tuple[float, int]

_caster = numpy.float64


# bins is a tuple of (cut point, count)
class Distogram:  # pragma: no cover
    """Compressed representation of a distribution."""

    __slots__ = "bins", "min", "max", "diffs", "min_diff", "_bin_count"

    def __init__(self, bin_count: int = BIN_COUNT):
        """Creates a new Distogram object

        Args:
            bin_count: [Optional] the number of bins to use.
            weighted_diff: [Optional] Whether to use weighted bin sizes.

        Returns:
            A Distogram object.
        """
        self.bins: List[Bin] = list()
        self.min: Optional[float] = None
        self.max: Optional[float] = None
        self.diffs: Optional[List[float]] = None
        self.min_diff: Optional[float] = None

        self._bin_count = bin_count

    ## all class methods below here have been added for Opteryx
    def dumps(self):  # pragma: no cover
        import orjson

        def handler(obj):
            if isinstance(obj, numpy.integer):
                return int(obj)
            if isinstance(obj, numpy.inexact):
                return float(obj)
            raise TypeError

        return orjson.dumps(self.dump(), default=handler)

    def dump(self):
        bin_vals, bin_counts = zip(*self.bins)
        bin_vals = numpy.array(bin_vals, dtype=numpy.float128)
        self.bins = list(zip(bin_vals, bin_counts))
        return {
            "bins": self.bins,
            "min": self.min,
            "max": self.max,
        }

    def __add__(self, operand):  # pragma: no cover
        dgram = merge(self, operand)
        # merge estimates min and max, so set them manually
        dgram.min = min(self.min, operand.min)
        dgram.max = max(self.max, operand.max)
        return dgram

    def bulkload(self, values):
        # To speed up bulk loads we use numpy to get a histogram at a higher resolution
        # and add this to the distogram.
        # Histogram gives us n+1 values, so we average consecutive values.
        # This ends up being an approximation of an approximation but 1000x faster.
        # The accuracy of this approach is poor on datasets with very low record counts,
        # but even if a bad decision is made on a table with 500 rows, the consequence
        # is minimal, if a bad decision is made on a table with 5m rows, it starts to
        # matter.
        if len(values) == 0:
            return
        bin_values, counts = numpy.unique(values, return_counts=True)
        if len(bin_values) > (self._bin_count * 5):
            counts, bin_values = numpy.histogram(values, self._bin_count * 5, density=False)
            bin_values = [bin_values[i] + bin_values[i + 1] / 2 for i in range(len(bin_values) - 1)]
        for index, count in enumerate(counts):
            if count > 0:
                update(
                    self,
                    value=bin_values[index],
                    count=count,
                )

        # we need to overwrite any range values as we've approximated the dataset
        if self.min is None:
            self.min = values.min()
            self.max = values.max()
        else:
            self.min = min(self.min, values.min())
            self.max = max(self.max, values.max())

    def count(self):
        return sum(f for _, f in self.bins)

    @property
    def max_bin_count(self):
        return self._bin_count

    @property
    def bin_count(self):
        return len(self.bins)


# added for opteryx
def load(dic):  # pragma: no cover
    if not isinstance(dic, dict):
        import orjson

        dic = orjson.loads(dic)
    dgram = Distogram()
    dgram.bins = dic["bins"]
    dgram.min = dic["min"]
    dgram.max = dic["max"]

    for i in range(len(dgram.bins) - 1):
        diff = dgram.bins[i][0] - dgram.bins[i - 1][0]
        dgram.diffs.append(diff)
    dgram.min_diff = min(dgram.min_diff)

    return dgram


def _linspace(start: float, stop: float, num: int) -> List[float]:  # pragma: no cover
    if num == 1:
        return [start, stop]
    step = (stop - start) / float(num)
    values = [start + step * i for i in range(num)]
    values.append(stop)
    return values


def _moment(x: List[float], counts: List[float], c: float, n: int) -> float:  # pragma: no cover
    """
    Calculates the k-th moment of the distribution using the formula:

    moment_k = sum((v - mean)**k * f) / sum(f)

    where v is the value of a bin, f is its frequency, and mean is the mean of
    the distribution.

    Args:
        h (Distogram): The input distribution.
        k (int): The order of the moment to calculate.

    Returns:
        float: The k-th moment of the distribution.

    Raises:
        ValueError: If the distribution has no bins.

    """
    m = sum(ci * (v - c) ** n for ci, v in zip(counts, x))
    return m / sum(counts)


def _update_diffs(h: Distogram, i: int) -> None:  # pragma: no cover
    if h.diffs is not None:
        update_min = False

        if i > 0:
            if h.diffs[i - 1] == h.min_diff:
                update_min = True

            h.diffs[i - 1] = h.bins[i][0] - h.bins[i - 1][0]
            if h.diffs[i - 1] < h.min_diff:
                h.min_diff = h.diffs[i - 1]

        if i < len(h.bins) - 1:
            if h.diffs[i] == h.min_diff:
                update_min = True

            h.diffs[i] = h.bins[i + 1][0] - h.bins[i][0]
            if h.diffs[i] < h.min_diff:
                h.min_diff = h.diffs[i]

        if update_min is True:
            h.min_diff = min(h.diffs)

    return


def _trim(h: Distogram) -> Distogram:  # pragma: no cover
    while len(h.bins) > h._bin_count:
        if h.diffs is not None:
            i = h.diffs.index(h.min_diff)
        else:
            diffs = [(i - 1, b[0] - h.bins[i - 1][0]) for i, b in enumerate(h.bins[1:], start=1)]
            i, _ = min(diffs, key=itemgetter(1))

        v1, f1 = h.bins[i]
        v2, f2 = h.bins.pop(i + 1)
        h.bins[i] = (v1 * f1 + v2 * f2) / (f1 + f2), f1 + f2

        if h.diffs is not None:
            h.diffs.pop(i)
            _update_diffs(h, i)
            h.min_diff = min(h.diffs)

    return h


def _trim_in_place(
    distogram: Distogram, new_value: float, new_count: int, bin_index: int
) -> Distogram:
    current_value, current_frequency = distogram.bins[bin_index]
    current_value = _caster(current_value)
    distogram.bins[bin_index] = (current_value * current_frequency + new_value * new_count) / (
        current_frequency + new_count
    ), current_frequency + new_count
    _update_diffs(distogram, bin_index)
    return distogram


def _compute_diffs(h: Distogram) -> List[float]:  # pragma: no cover
    diffs = [v2 - v1 for (v1, _), (v2, _) in zip(h.bins[:-1], h.bins[1:])]
    h.min_diff = min(diffs)

    return diffs


def _search_in_place_index(h: Distogram, new_value: float, index: int) -> int:  # pragma: no cover
    if h.diffs is None:
        h.diffs = _compute_diffs(h)

    if index > 0:
        diff1 = new_value - h.bins[index - 1][0]
        diff2 = h.bins[index][0] - new_value

        i_bin, diff = (index - 1, diff1) if diff1 < diff2 else (index, diff2)

        return i_bin if diff < h.min_diff else -1

    return -1


def update(h: Distogram, value: float, count: int = 1) -> Distogram:  # pragma: no cover
    """Adds a new element to the distribution.

    Args:
        h: A Distogram object.
        value: The value to add on the histogram.
        count: [Optional] The number of times that value must be added.

    Returns:
        A Distogram object where value as been processed.

    Raises:
        ValueError if count is not strictly positive.
    """
    if count <= 0:
        raise ValueError("count must be strictly positive")

    index = 0
    if len(h.bins) > 0:
        if value <= h.bins[0][0]:
            index = 0
        elif value >= h.bins[-1][0]:
            index = -1
        else:
            index = bisect_left(h.bins, (value, 1))

        vi, fi = h.bins[index]
        if vi == value:
            h.bins[index] = (_caster(vi), fi + count)
            return h

    if index > 0 and len(h.bins) >= h._bin_count:
        in_place_index = _search_in_place_index(h, value, index)
        if in_place_index > 0:
            h = _trim_in_place(h, value, count, in_place_index)
            return h

    if index == -1:
        h.bins.append((_caster(value), count))
        if h.diffs is not None:
            diff = h.bins[-1][0] - h.bins[-2][0]
            h.diffs.append(diff)
            h.min_diff = min(h.min_diff, diff)
    else:
        h.bins.insert(index, (_caster(value), count))
        if h.diffs is not None:
            h.diffs.insert(index, 0)
            _update_diffs(h, index)

    if (h.min is None) or (h.min > value):
        h.min = value
    if (h.max is None) or (h.max < value):
        h.max = value

    h = _trim(h)


def merge(h1: Distogram, h2: Distogram) -> Distogram:  # pragma: no cover
    """Merges two Distogram objects

    Args:
        h1: First Distogram.
        h2: Second Distogram.

    Returns:
        A Distogram object being the composition of h1 and h2. The number of
        bins in this Distogram is equal to the number of bins in h1.
    """
    h = reduce(
        lambda residual, b: update(residual, *b),
        h2.bins,
        h1,
    )
    return h


def count_at(h: Distogram, value: float):  # pragma: no cover
    """Counts the number of elements present in the distribution up to value.

    Args:
        h: A Distogram object.
        value: The value up to what elements must be counted.

    Returns:
        An estimation of the real count, computed from the compressed
        representation of the distribution. Returns None if the Distogram
        object contains no element or value is outside of the distribution
        bounds.
    """
    if len(h.bins) == 0:
        return None

    if value < h.min or value > h.max:
        return None

    if value == h.min:
        return 0

    if value == h.max:
        return count(h)

    v0, f0 = h.bins[0]
    vl, fl = h.bins[-1]
    if value <= v0:  # left
        ratio = (value - h.min) / (v0 - h.min)
        result = ratio * v0 / 2
    elif value >= vl:  # right
        ratio = (value - vl) / (h.max - vl)
        result = (1 + ratio) * fl / 2
        result += sum((f for _, f in h.bins[:-1]))
    else:
        i = sum(((value > v) for v, _ in h.bins)) - 1
        vi, fi = h.bins[i]
        vj, fj = h.bins[i + 1]

        mb = fi + (fj - fi) / (vj - vi) * (value - vi)
        result = (fi + mb) / 2 * (value - vi) / (vj - vi)
        result += sum((f for _, f in h.bins[:i]))

        result = result + fi / 2

    return result


def count(h: Distogram) -> float:  # pragma: no cover
    """Counts the number of elements in the distribution.

    Args:
        h: A Distogram object.

    Returns:
        The number of elements in the distribution.
    """
    return sum(f for _, f in h.bins)


def bounds(h: Distogram) -> Tuple[float, float]:  # pragma: no cover
    """Returns the min and max values of the distribution.

    Args:
        h: A Distogram object.

    Returns:
        A tuple containing the minimum and maximum values of the distribution.
    """
    return h.min, h.max


def mean(h: Distogram) -> float:  # pragma: no cover
    """Returns the mean of the distribution.

    Args:
        h: A Distogram object.

    Returns:
        An estimation of the mean of the values in the distribution.
    """
    p, m = zip(*h.bins)
    return _moment(p, m, 0, 1)


def variance(h: Distogram) -> float:  # pragma: no cover
    """Returns the variance of the distribution.

    Args:
        h: A Distogram object.

    Returns:
        An estimation of the variance of the values in the distribution.
    """
    p, m = zip(*h.bins)
    return _moment(p, m, mean(h), 2)


def stddev(h: Distogram) -> float:  # pragma: no cover
    """Returns the standard deviation of the distribution.

    Args:
        h: A Distogram object.

    Returns:
        An estimation of the standard deviation of the values in the
        distribution.
    """
    return math.sqrt(variance(h))


def histogram(
    h: Distogram, bin_count: int = 100
) -> Tuple[List[float], List[float]]:  # pragma: no cover
    """Returns a histogram of the distribution in numpy format.

    Args:
        h: A Distogram object.
        bin_count: [Optional] The number of bins in the histogram.

    Returns:
        An estimation of the histogram of the distribution, or None
        if there is not enough items in the distribution.
    """
    if len(h.bins) < bin_count:
        return None

    bin_bounds = _linspace(h.min, h.max, num=bin_count)
    counts = [count_at(h, e) for e in bin_bounds]
    counts = [new - last for new, last in zip(counts[1:], counts[:-1])]
    u = tuple([counts, bin_bounds])
    return u


def frequency_density_distribution(
    h: Distogram,
) -> Tuple[List[float], List[float]]:  # pragma: no cover
    """Returns a histogram of the distribution

    Args:
        h: A Distogram object.

    Returns:
        An estimation of the frequency density distribution, or None if
        there are not enough values in the distribution.
    """

    if count(h) < 2:
        return None

    bin_bounds = [float(i[0]) for i in h.bins]
    bin_widths = [(bin_bounds[i] - bin_bounds[i - 1]) for i in range(1, len(bin_bounds))]
    counts = [0]
    counts.extend([count_at(h, e) for e in bin_bounds[1:]])
    densities = [
        (new - last) / delta for new, last, delta in zip(counts[1:], counts[:-1], bin_widths)
    ]
    return (densities, bin_bounds)


def quantile(h: Distogram, value: float) -> Optional[float]:  # pragma: no cover
    """Returns a quantile of the distribution

    Args:
        h: A Distogram object.
        value: The quantile to compute. Must be between 0 and 1

    Returns:
        An estimation of the quantile. Returns None if the Distogram
        object contains no element or value is outside of [0, 1].
    """
    if len(h.bins) == 0:
        return None

    if not (0 <= value <= 1):
        return None

    total_count = count(h)
    q_count = int(total_count * value)
    v0, f0 = h.bins[0]
    vl, fl = h.bins[-1]

    if q_count <= (f0 / 2):  # left values
        fraction = q_count / (f0 / 2)
        result = h.min + (fraction * (v0 - h.min))

    elif q_count >= (total_count - (fl / 2)):  # right values
        base = q_count - (total_count - (fl / 2))
        fraction = base / (fl / 2)
        result = vl + (fraction * (h.max - vl))

    else:
        mb = q_count - f0 / 2
        mids = [(fi + fj) / 2 for (_, fi), (_, fj) in zip(h.bins[:-1], h.bins[1:])]
        i, _ = next(filter(lambda i_f: mb < i_f[1], enumerate(accumulate(mids))))

        (vi, _), (vj, _) = h.bins[i], h.bins[i + 1]
        fraction = (mb - sum(mids[:i])) / mids[i]
        result = vi + (fraction * (vj - vi))

    return result
