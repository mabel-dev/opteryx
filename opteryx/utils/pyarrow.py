import pyarrow
from typing import Iterable, List


def fetchmany(pages: Iterable, size: int = 5) -> List[dict]:
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
    return fetchmany(pages=pages, limit=1).pop()


def fetchall(pages) -> List[dict]:
    return fetchmany(pages=pages, size=-1)
