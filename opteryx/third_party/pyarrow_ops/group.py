import numpy as np
import pyarrow as pa
from .helpers import combine_column, columns_to_array, groupify_array

# Grouping / groupby methods
agg_methods = {
    "sum": np.sum,
    "max": np.max,
    "min": np.min,
    "mean": np.mean,
    "median": np.median,
}


def add_agg_method(self, name, method):
    def f(agg_columns=[]):
        methods = {
            col: method
            for col in (agg_columns if agg_columns else self.table.column_names)
            if col not in self.columns
        }
        return self.aggregate(methods=methods)

    setattr(self, name, f)


class Grouping:
    def __init__(self, table, columns):
        self.table = table
        self.columns = list(set(columns))

        # Initialize array + groupify
        self.arr = columns_to_array(table, columns)
        self.dic, self.counts, self.sort_idxs, self.bgn_idxs = groupify_array(self.arr)
        self.set_methods()

    def __iter__(self):
        for i in range(len(self.dic)):
            idxs = self.sort_idxs[self.bgn_idxs[i] : self.bgn_idxs[i] + self.counts[i]]
            yield {
                k: v[0]
                for k, v in self.table.select(self.columns)
                .take([self.sort_idxs[self.bgn_idxs[i]]])
                .to_pydict()
                .items()
            }, self.table.take(idxs)

    # Aggregation methods
    def set_methods(self):
        for k, m in agg_methods.items():
            add_agg_method(self, k, m)

    def aggregate(self, methods):
        # Create index columns
        table = self.table.select(self.columns).take(self.sort_idxs[self.bgn_idxs])

        data = {k: self.table.column(k).to_numpy() for k in methods.keys()}
        for col, f in methods.items():
            vf = np.vectorize(f, otypes=[object])
            agg_arr = vf(np.split(data[col][self.sort_idxs], self.bgn_idxs[1:]))
            table = table.append_column(col, pa.array(agg_arr))
        return table

    def agg(self, methods):
        methods = {col: agg_methods[m] for col, m in methods.items()}
        return self.aggregate(methods=methods)


def groupby(table, by):
    return Grouping(table, by)
