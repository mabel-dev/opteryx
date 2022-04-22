# Caching

## Buffer Pool

Read cache, saving a copy of data stored remotely to a faster store.

### In Memory Cache

Uses the cache local to the machine to cache pages. Fastest, but most limiting and volatile.

### Memcached Cache

Uses a Memcached instance to cache pages. Is a good option when remote reads are slow, for example from GCS or S3.
