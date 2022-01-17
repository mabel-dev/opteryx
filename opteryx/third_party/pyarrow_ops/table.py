import pyarrow as pa
from pyarrow_ops import join, filters, groupby, drop_duplicates, head

# Table wrapper: does not work because pa.Table.from_pandas/from_arrays/from_pydict always returns pa.Table
class Table(pa.Table):
    def __init__(*args, **kwargs):
        super(Table, self).__init__(*args, **kwargs)

    def join(self, right, on):
        return join(self, right, on)

    def filters(self, filters):
        return filters(self, filters)

    def groupby(self, by):
        return groupby(self, by)

    def drop_duplicates(self, on=[], keep="last"):
        return drop_duplicates(self, on, keep)

    def head(self, n=5):
        return head(self, n)


# Add methods to class pa.Table or instances of pa.Table: does not work because pyarrow.lib.Table is build in C
def add_table_methods(table):
    def join(self, right, on):
        return join(self, right, on)

    table.join = join

    def filters(self, filters):
        return filters(self, filters)

    table.filters = filters

    def groupby(self, by):
        return groupby(self, by)

    table.groupby = groupby

    def drop_duplicates(self, on=[], keep="last"):
        return drop_duplicates(self, on, keep)

    table.drop_duplicates = drop_duplicates

    def head(self, n=5):
        return head(self, n)

    table.head = head
