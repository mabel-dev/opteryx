# Advanced 

Hints are used to force the planner, optimizer or the executor to make specific decisions. If a hint is not recognized, it is ignored.

## WITH hints

~~~
FROM dataset WITH(NO_CACHE)
~~~

Instructs blob/o not use cache, regardless of settings

~~~
FROM dataset WITH(NO_PARTITION)
~~~

Disable partitioning