from ...data.internals.dictset import DictSet


BAR_CHARS = [r" ", r"▁", r"▂", r"▃", r"▄", r"▅", r"▆", r"▇", r"█"]


class Histogram:
    def __init__(self, values, *, key: str, number_of_bins: int = 10):

        if isinstance(values, DictSet):
            values = values.collect_list(key)

        self.bins = [0] * number_of_bins
        mn = min(values)
        mx = max(values)
        interval = ((1 + mx) - mn) / (number_of_bins - 1)

        for v in values:
            self.bins[int((v - mn) / interval)] += 1

    def __repr__(self):
        """
        Draws a pre-binned set off histogram data
        """
        mx = max(self.bins)
        bar_height = mx / 8
        if bar_height == 0:
            return ">" + " " * len(self.bins) + "<"

        histogram = ""
        for value in self.bins:
            if value == 0:
                histogram += BAR_CHARS[0]
            else:
                height = int(value / bar_height)
                histogram += BAR_CHARS[height]

        return f">{histogram}<"
