import pyarrow as pa
import numpy as np
import pyarrow.compute as c

# Cleaning functions
def clean_num(arr, impute=0.0, clip_min=None, clip_max=None):
    return (
        pa.array(
            np.nan_to_num(
                arr.to_numpy(zero_copy_only=False).astype(np.float64), nan=impute
            ).clip(clip_min, clip_max)
        ),
    )


def clean_cat(arr, categories=[]):
    arr = arr.cast(pa.string()).dictionary_encode()
    dic = arr.dictionary.to_pylist()
    if categories:
        d = {
            i: (categories.index(v) + 1 if v in categories else 0)
            for i, v in enumerate(dic)
        }
        d[-1] = 0  # NULLs -> 0
        return (
            pa.array(np.vectorize(d.get)(arr.indices.fill_null(-1).to_numpy())),
            ["Unknown"] + categories,
        )
    else:
        return (
            c.add(arr.indices, pa.array([1], type=pa.int32())[0]).fill_null(0),
            ["Unknown"] + dic,
        )


def clean_hot(arr, categories=[], drop_first=False):
    arr = arr.cast(pa.string())
    if categories:
        clns = [c.equal(arr, v).fill_null(False) for v in categories]
    else:
        categories = [u for u in arr.unique().to_pylist() if u]
        clns = [c.equal(arr, v).fill_null(False) for v in categories]
    return clns[(1 if drop_first else 0) :], categories[(1 if drop_first else 0) :]


# Cleaning Classes
class NumericalColumn:
    def __init__(
        self, name, impute="mean", clip=True, v_min=None, v_mean=None, v_max=None
    ):
        self.name, self.impute, self.clip = name, impute, clip
        self.measured = any([v_min, v_mean, v_max])
        self.mean, self.min, self.max = (v_mean or 0), (v_min or 0), (v_max or 0)

    def to_dict(self):
        return {
            "name": self.name,
            "type": "numerical",
            "impute": self.impute,
            "clip": self.clip,
            "v_min": self.min,
            "v_mean": self.mean,
            "v_max": self.max,
        }

    def update(self, arr):
        self.mean = float(c.mean(arr).as_py())
        minmax = c.min_max(arr)
        self.min, self.max = float(minmax["min"].as_py()), float(minmax["max"].as_py())

    def value(self):
        if self.impute == "mean":
            return self.mean
        elif self.impute == "min":
            return self.min
        elif self.impute == "max":
            return self.max
        else:
            raise Exception("{} is not a valid impute method".format(self.impute))

    def clean(self, arr):
        if not self.measured:
            self.update(arr)
        (cln,) = clean_num(
            arr,
            impute=self.value(),
            clip_min=(self.min if self.clip else None),
            clip_max=(self.max if self.clip else None),
        )
        return cln, None


class CategoricalColumn:
    def __init__(self, name, method, categories=[]):
        self.name, self.method, self.categories = name, method, categories
        self.measured = True if categories else False

    def to_dict(self):
        return {
            "name": self.name,
            "type": "categorical",
            "method": self.method,
            "categories": self.categories,
        }

    def update(self, categories):
        self.categories = self.categories + [
            c for c in categories if c not in self.categories
        ]

    def clean(self, arr):
        if self.method == "one_hot":
            cln, cats = clean_hot(arr, categories=self.categories)
        else:
            cln, cats = clean_cat(arr, categories=self.categories)
        if not self.measured:
            self.categories = cats
        return cln, cats


class TableCleaner:
    def __init__(self):
        self.columns = []

    def to_dict(self):
        return [column.to_dict() for column in self.columns]

    def from_dict(self, columns):
        for column in columns:
            t = column.pop("type")
            if t == "numerical":
                self.columns.append(NumericalColumn(**column))
            else:
                self.columns.append(CategoricalColumn(**column))
        return self

    def register_numeric(self, name, impute="mean", clip=True):
        self.columns.append(NumericalColumn(name, impute, clip))

    def register_label(self, name, categories=[]):
        self.columns.append(
            CategoricalColumn(name, method="label", categories=categories)
        )

    def register_one_hot(self, name, categories=[]):
        self.columns.append(
            CategoricalColumn(name, method="one_hot", categories=categories)
        )

    def clean_column(self, table, column):
        arr = table.column(column.name).combine_chunks()
        cln, cats = column.clean(arr)
        if column.__dict__.get("method", "") == "one_hot":
            return [column.name + "_" + cat for cat in cats], cln
        else:
            return [column.name], [cln]

    def clean_table(self, table, label=None):
        keys, arrays = [], []
        for column in self.columns:
            k, a = self.clean_column(table, column)
            keys.extend(k)
            arrays.extend(a)
        if label:
            return pa.Table.from_arrays(arrays, names=keys), table.column(label)
        else:
            return pa.Table.from_arrays(arrays, names=keys)

    def split(self, X, y=None, test_size=0.2):
        mask = np.random.rand(X.num_rows) > test_size
        while np.all(mask):  # [True, True, True] is invalid
            mask = np.random.rand(X.num_rows) > test_size
        idxs, not_idxs = np.where(mask)[0], np.where(~mask)[0]
        return (
            X.take(idxs),
            X.take(not_idxs),
            y.take(idxs),
            y.take(not_idxs),
        )  # X_train, X_test, y_train, y_test
