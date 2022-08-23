# Advanced Set Up

## Configuration File

Configuration values are set a `opteryx.yaml` file in the directory the application is run from.

 Key                       | Default     | Description
-------------------------- | ----------: | -----------
`INTERNAL_BATCH_SIZE`      | 500         | Batch size for left-table of a join processes
`MAX_JOIN_SIZE`            | 1000000     | Maximum records created in a CROSS JOIN frame
`MEMCACHED_SERVER`         | _not set_   | Address of Memcached server, in `IP:PORT` format
`MAX_SUB_PROCESSES`        | Physical CPU count | Subprocesses used to parallelize processing
`BUFFER_PER_SUB_PROCESS`   | 100000000   | Memory to allocate per subprocess
`MAXIMUM_SECONDS_SUB_PROCESSES_CAN_RUN ` | 3600 | Time to wait before killing subprocesses
`DATASET_PREFIX_MAPPING`   | _ | reader
`PARTITION_SCHEME`         | mabel       | How the blob/file data is partitioned
`MAX_SIZE_SINGLE_CACHE_ITEM` | 1048576   | The maximum size of an item to store in the buffer cache
`PAGE_SIZE`                | 67108864    | The size to try to make data pages as they are processed

## Environment Variables

The environment is the preferred location for secrets, although the engine will read `.env` files if [dotenv](https://pypi.org/project/python-dotenv/) has also been installed.

- `MONGO_CONNECTION`
- `MINIO_END_POINT`
- `MINIO_ACCESS_KEY`
- `MINIO_SECRET_KEY`
- `MINIO_SECURE`

## Caching

### Buffer Pool

The observed bottleneck for query performance is almost always IO. It is not uncommon for 90% of the execution time is initial load of data - this can vary considerably by storage and query.

Opteryx implements a cache similar in concept to a [Buffer Pool](https://www.ibm.com/docs/en/db2/11.5?topic=databases-buffer-pools) in a traditional RDBMS to improve data load performance. However, rather than always residing in main memory, the Buffer Pool can reside in any memory store faster than main storage access. This configuration is managed by the user.

The Buffer Pool currently has two implementations, In Memory Cache and Memcached Cache. When your main storage is local disk, using Memcached as your Buffer Pool is unlikely to provide significant performance improvement, however  the In Memory Cache may; when using remote storage such as S3 or GCS, Memcached Cache can provide significant improvements. However, as will all optimization, test in your unique set of circumstances before assuming it to be true.

**In Memory Cache**

Uses the main memory of the host machine to cache pages. This is usually fastest, but most limiting and volatile. This is a good fit for high specification hosts.

The size of the cache is set by the number of pages to hold in memory. No checks are made if the pages actually fit in memory and setting the cache too large, or running on a host where there is high contention for memory where memory is swapped to disk, may result in negative performance.

**Memcached Cache**

Uses a Memcached instance to cache pages. Is a good option when remote reads are slow, for example from GCS or S3.

This is also recommended in an environment where multiple servers, or container instances, may be serving customers. Here, the shared cache allows users to benefit from caching even on their first query if another user's query has populated the cache with the files being read.
