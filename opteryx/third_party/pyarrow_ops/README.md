# Pyarrow ops
Pyarrow ops is Python libary for data crunching operations directly on the pyarrow.Table class, implemented in numpy & Cython. For convenience, function naming and behavior tries to replicates that of the Pandas API. The Join / Groupy performance is slightly slower than that of pandas, especially on multi column joins.

Current use cases:
- Data operations like joins, groupby (aggregations), filters & drop_duplicates
- (Very fast) reusable pre-processing for ML applications

## Installation

Use the package manager [pip](https://pip.pypa.io/en/stable/) to install pyarrow_ops.

```bash
pip install pyarrow_ops
```

## Usage
See test_*.py for runnable test examples

Data operations:
```python
import pyarrow as pa 
from pyarrow_ops import join, filters, groupby, head, drop_duplicates

# Create pyarrow.Table
t = pa.Table.from_pydict({
    'Animal': ['Falcon', 'Falcon', 'Parrot', 'Parrot', 'Parrot'],
    'Max Speed': [380., 370., 24., 26., 24.]
})
head(t) # Use head to print, like df.head()

# Drop duplicates based on column values
d = drop_duplicates(t, on=['Animal'], keep='first')

# Groupby iterable
for key, value in groupby(t, ['Animal']):
    print(key)
    head(value)

# Group by aggregate functions
g = groupby(t, ['Animal']).sum()
g = groupby(t, ['Animal']).agg({'Max Speed': 'max'})

# Use filter predicates using list of tuples (column, operation, value)
f = filters(t, [('Animal', 'not in', ['Falcon', 'Duck']), ('Max Speed', '<', 25)])

# Join operations (currently performs inner join)
t2 = pa.Table.from_pydict({
    'Animal': ['Falcon', 'Parrot'],
    'Age': [10, 20]
})
j = join(t, t2, on=['Animal'])
```

ML Preprocessing (note: personal tests showed ~5x speed up compared to pandas on large datasets)
```python
import pyarrow as pa 
from pyarrow_ops import head, TableCleaner

# Training data
t1 = pa.Table.from_pydict({
    'Animal': ['Falcon', 'Falcon', 'Parrot', 'Parrot', 'Parrot'],
    'Max Speed': [380., 370., None, 26., 24.],
    'Value': [2000, 1500, 10, 30, 20],
})

# Create TableCleaner & register columns to be processed
cleaner = TableCleaner()
cleaner.register_numeric('Max Speed', impute='min', clip=True)
cleaner.register_label('Animal', categories=['Goose', 'Falcon'])
cleaner.register_one_hot('Animal')

# Clean table and split into train/test
X, y = cleaner.clean_table(t1, label='Value')
X_train, X_test, y_train, y_test = cleaner.split(X, y)

# Train a model + Save cleaner settings
cleaner_dict = cleaner.to_dict()

# Prediction data
t2 = pa.Table.from_pydict({
    'Animal': ['Falcon', 'Goose', 'Parrot', 'Parrot'],
    'Max Speed': [380., 10., None, 26.]
})
new_cleaner = TableCleaner().from_dict(cleaner_dict)
X_pred = new_cleaner.clean_table(t2)
```

### To Do's
- [x] Improve groupby speed by not create copys of table
- [x] Add ML cleaning class
- [x] Improve speed of groupby by avoiding for loops
- [x] Improve join speed by moving code to C
- [ ] Add unit tests using pytest
- [ ] Add window functions on groupby
- [ ] Add more join options (left, right, outer, full, cross)
- [ ] Allow for functions to be classmethods of pa.Table* (t.groupby(...))

*One of the main difficulties is that the pyarrow classes are written in C and do not have a __dict__ method, this hinders inheritance and adding classmethods.

## Relation to pyarrow
In the future many of these functions might be obsolete by enhancements in the pyarrow package, but for now it is a convenient alternative to switching back and forth between pyarrow and pandas.

## Contributing
Pull requests are very welcome, however I believe in 80% of the utility in 20% of the code. I personally get lost reading the tranches of the pandas source code. If you would like to seriously improve this work, please let me know!