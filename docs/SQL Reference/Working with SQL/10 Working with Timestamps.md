# Working with Timestamps

## Actions

### Add/Subtract

~~~
DATEDIFF(part, start, end)
~~~
<!---
### Construct
--->
### Extract

~~~
EXTRACT(part FROM timestamp)
~~~
~~~
DATE(timestamp)
~~~

### FORMAT

~~~
DATE_FORMAT(timestamp, format)
~~~

### Parse

~~~
CAST(field AS TIMESTAMP)
~~~
~~~
TIMESTAMP(field)
~~~

### Truncate

~~~
DATE_TRUNC(part, timestamp)
~~~

### Generate

~~~
current_date
~~~
~~~
current_time
~~~
~~~
YESTERDAY()
~~~
~~~
TIME()
~~~

Note that `current_date` and `current_time` support being called without parenthesis.