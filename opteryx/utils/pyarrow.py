import pyarrow
from typing import Iterable, Iterator, List, Union, Generator


def fetchmany(pages: Iterable, size: int = 5) -> List[dict]:  # type:ignore
    """
    This is the fastest way I've found to do this, on my computer it's about
    14,000 rows per second - fast enough for my use case.
    """

    def _inner_row_reader():
        for page in pages:
            for index in range(page.num_rows):
                row = page.take([index]).to_pydict()
                for k, v in row.items():
                    row[k] = v[0]
                yield row

    index = -1
    for index, row in enumerate(_inner_row_reader()):
        if index == size:
            return
        yield row

    if index < 0:
        yield {}


def fetchone(pages: Iterable) -> dict:
    return fetchmany(pages=pages, size=1).pop()


def fetchall(pages) -> List[dict]:
    return fetchmany(pages=pages, size=-1)


class Grouping:
    def __init__(self, table, columns):
        # This is a reduced version of the groupby function in
        # https://github.com/TomScheffers/pyarrow_ops
        from opteryx.third_party.pyarrow_ops.helpers import (
            columns_to_array,
            groupify_array,
        )

        self.table = table

        # Initialize array + groupify
        self.arr = columns_to_array(table, columns)
        # dic - the groups
        # counts - the number of items for each group
        # sort indexes - the indexes which would sort this table
        # begin indexes - the first entry for each group
        self.dic, self.counts, self.sort_idxs, self.bgn_idxs = groupify_array(self.arr)

    def __iter__(self):
        for i in range(len(self.dic)):
            idxs = self.sort_idxs[self.bgn_idxs[i] : self.bgn_idxs[i] + self.counts[i]]
            yield self.table.take(idxs)


def groupby(table, by):
    return Grouping(table, by)
