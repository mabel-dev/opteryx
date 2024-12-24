# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
This is a virtual dataset which is calculated at access time.
"""

import time

from orso.schema import FlatColumn
from orso.schema import RelationSchema
from orso.types import OrsoTypes

__all__ = ("read", "schema")


def read(end_date=None, variables={}):
    import pyarrow

    from opteryx import system_statistics
    from opteryx.shared.buffer_pool import BufferPool

    bufferpool = BufferPool()
    pool = bufferpool._memory_pool

    # fmt:off
    buffer = [
        {"key": "bufferpool_commits", "value": str(pool.commits)},
        {"key": "bufferpool_failed_commits", "value": str(pool.failed_commits)},
        {"key": "bufferpool_reads", "value": str(pool.reads)},
        {"key": "bufferpool_read_locks", "value": str(pool.read_locks)},
        {"key": "bufferpool_compaction_l1", "value": str(pool.l1_compaction)},
        {"key": "bufferpool_compaction_l2", "value": str(pool.l2_compaction)},
        {"key": "bufferpool_releases", "value": str(pool.releases)},
        {"key": "bufferpool_capacity", "value": str(pool.size)},
        {"key": "bufferpool_free", "value": str(pool.available_space())},
        {"key": "bufferpool_items", "value": str(len(pool.used_segments))},
        {"key": "queries_executed", "value": str(system_statistics.queries_executed)},
        {"key": "uptime_seconds","value": str((time.time_ns() - system_statistics.start_time) / 1e9)},
        {"key": "io_wait_seconds", "value": str(system_statistics.io_wait_seconds)},
        {"key": "cpu_wait_seconds", "value": str(system_statistics.cpu_wait_seconds)},
        {"key": "origin_reads", "value": str(system_statistics.origin_reads)},
        {"key": "remote_cache_reads", "value": str(system_statistics.remote_cache_reads)},
        {"key": "remote_cache_commits", "value": str(system_statistics.remote_cache_commits)},
    ]
    # fmt:on

    return pyarrow.Table.from_pylist(buffer)


def schema():
    # fmt:off
    return  RelationSchema(
        name="$statistics",
        columns=[
            FlatColumn(name="key", type=OrsoTypes.VARCHAR),
            FlatColumn(name="value", type=OrsoTypes.VARCHAR),
        ],
    )
    # fmt:on
