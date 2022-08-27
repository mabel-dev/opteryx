import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

import opteryx
from opteryx.connectors import DiskStorage


def simple_query():

    conn = opteryx.connect(reader=DiskStorage(), partition_scheme=None)

    # SELECT EVERYTHING
    cur = conn.cursor()
    cur.execute(
        f"SELECT * FROM tests.data.formats.jsonl WHERE user_id = 762916610478747648"
    )
    [a for a in cur.fetchall()]


if __name__ == "__main__":
    import cProfile
    from pstats import Stats

    pr = cProfile.Profile()
    pr.enable()

    simple_query()

    pr.disable()
    stats = Stats(pr)
    stats.sort_stats("tottime").print_stats(10)
