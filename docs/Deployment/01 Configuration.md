# Configuration

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

The environment is the preferred location for secrets, although the engine will read `.env` files if [dotenv](https://pypi.org/project/python-dotenv/) is installed.

- `MONGO_CONNECTION`
- `MINIO_END_POINT`
- `MINIO_ACCESS_KEY`
- `MINIO_SECRET_KEY`
- `MINIO_SECURE`