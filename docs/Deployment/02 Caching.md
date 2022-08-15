# Caching

## Buffer Pool

Opteryx implements a cache similar to a [Buffer Pool](https://www.ibm.com/docs/en/db2/11.5?topic=databases-buffer-pools) in a traditional RDBMS. However, rather than always residing in main memory, the Buffer Pool can reside in any memory store faster than main storage access. This configuration is managed by the user.

The Buffer Pool currently has two implementations, In Memory Cache and Memcached Cache. When your main storage is local disk, using Memcached as your Buffer Pool is unlikely to provide significant performance improvement, however the In Memory Cache may; when using remote storage such as S3 or GCS, Memcached Cache can provide significant improvements.

### In Memory Cache

Uses the main memory of the host machine to cache pages. This is usually fastest, but most limiting and volatile. This is a good fit for high specification hosts.

The size of the cache is set by the number of pages to hold in memory. No checks are made if the pages actually fit in memory and setting the cache too large, or running on a host where there is high contention for memory where memory is swapped to disk, may result in negative performance.

### Memcached Cache

Uses a Memcached instance to cache pages. Is a good option when remote reads are slow, for example from GCS or S3.

This is also recommended in an environment where multiple servers, or container instances, may be serving customers. Here, the shared cache allows users to benefit from caching even on their first query.
