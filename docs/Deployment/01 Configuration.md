# Configuration

Configuration values are set a `opteryx.yaml` file in the directory the application is run from.

## Properties

Key | Default | Description
--- | ------: | -----------
`INTERNAL_BATCH_SIZE` | 500 | Batch size for left-table of a join processes
`MAX_JOIN_SIZE` | 1000000 | Maximum records created in a CROSS JOIN frame
`MEMCACHED_SERVER` | _<none>_ | Address of Memcached server, in `IP:PORT` format
`MAX_SUB_PROCESSES` | Physical CPU count | Subprocesses used to parallelize processing
`BUFFER_PER_SUB_PROCESS` | 100000000 | Memory to allocate per subprocess
`MAXIMUM_SECONDS_SUB_PROCESSES_CAN_RUN ` | 3600 | Time to wait before killing subprocesses