# Advanced 

Hints are used to force the planner, optimizer or the executor to make specific decisions. If a hint is not recognized, it is ignored by the planner and executor, however is reported in the warnings.

## WITH hints

~~~
FROM dataset WITH(NO_CACHE)
~~~

Instructs blob/file reader to not use cache, regardless of other settings.

~~~
FROM dataset WITH(NO_PARTITION)
~~~

Instructs the blob/file reader to not use partitioning, regardless of other settings.