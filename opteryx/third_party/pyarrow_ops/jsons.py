import pyarrow as pa
import json
import numpy as np


def str_to_table(arr):
    arr = arr.to_numpy()
    arr = np.vectorize(json.loads)(arr)
    return pa.Table.from_pydict({k: [dic.get(k, None) for dic in arr] for k in arr[0]})
