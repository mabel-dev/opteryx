from pyarrow import Table
from typing import List


def fetchmany(relation: Table, size: int = 5, offset: int = 0) -> List[dict]:
    if relation.num_rows == 0 or offset > relation.num_rows:
        return []

    def _inner(t):
        for index in range(t.num_rows):
            yield {k: v[index] for k, v in t.to_pydict().items()}

    return list(_inner(relation.slice(offset=offset, length=size)))


def fetchone(relation: Table, offset: int = 0) -> dict:
    return fetchmany(relation=relation, offset=offset).pop()


def fetchall(relation) -> List[dict]:
    return fetchmany(relation=relation, size=relation.num_rows, offset=0)
