import time
import numpy as np
import pyarrow as pa
from . import groupby, join, head, drop_duplicates

# Generate ids
left_size = int(1e4)
right_size = int(1e5)

# Create table
ids = np.random.choice(np.arange(left_size), size=left_size, replace=False)
l = pa.Table.from_arrays(
    [ids, np.random.randint(0, 10000, size=(left_size))], names=["id", "salary"]
)
head(l)
r = pa.Table.from_arrays(
    [
        np.random.choice(ids, size=(right_size)),
        np.random.randint(0, 20, size=(right_size)),
    ],
    names=["id", "age_children"],
)
head(r)

# Pyarrow ops
ti = time.time()
j = join(l, r, on=["id"])
print("Pyarrow ops join took:", time.time() - ti)

ti = time.time()
d = drop_duplicates(j, on=["id"])
print("Pyarrow ops drop_duplicates took:", time.time() - ti)

tg = time.time()
g = groupby(j, by=["id"]).agg({"age_children": "mean"})
print("Pyarrow ops groupby took:", time.time() - tg)

# Pandas
dfl, dfr = l.to_pandas(), r.to_pandas()

ti = time.time()
dfj = dfl.merge(dfr, how="left", left_on="id", right_on="id")
print("Pandas merge took:", time.time() - ti)

ti = time.time()
dfj = dfj.drop_duplicates(subset=["id"])
print("Pandas drop_duplicates took:", time.time() - ti)

tg = time.time()
dfg = dfj.groupby(["id"]).agg({"age_children": "mean"})
print("Pandas groupby took:", time.time() - tg)
