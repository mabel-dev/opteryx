# Advanced 

## WITH hints

Hints are used to force the planner, optimizer or the executor to make specific decisions. If a hint is not recognized, it is ignored by the planner and executor, however is reported in the warnings.

!!! Note
    Hints use the keyword `WITH` which is also the keyword for CTEs, this information relates to hints and not CTEs.

~~~
FROM dataset WITH(NO_CACHE)
~~~

Instructs blob/file reader to not use cache, regardless of other settings.

~~~
FROM dataset WITH(NO_PARTITION)
~~~

Instructs the blob/file reader to not use partitioning, regardless of other settings.

~~~
FROM dataset WITH(NO_PUSH_PROJECTION)
~~~

Instructs the blob/file reader not to try to prune columns at read time.

~~~
FROM dataset WITH(PARALLEL_READ)
~~~

Instructs the blob/file reader to try to use multiple processes to download files (experimental). Expected to be be `NO_PARALLEL_READ` when out of experimental mode.