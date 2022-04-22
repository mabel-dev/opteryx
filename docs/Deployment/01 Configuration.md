# Configuration

Configuration values are set in the operating system environment or in a `.env` file.

## Properties

Key | Default | Description
--- | ------: | -----------
`INTERNAL_BATCH_SIZE` | 500 | Batch size for left-table of a join processes
`MAX_JOIN_SIZE` | 1000000 | Maximum records created in a CROSS JOIN frame
`MEMCACHED_SERVER` | _<none>_ | Address of Memcached server, in `IP:PORT` format