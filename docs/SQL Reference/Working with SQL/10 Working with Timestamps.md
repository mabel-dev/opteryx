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

<!---
### Format
--->
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
NOW()
~~~
~~~
TODAY()
~~~
~~~
YESTERDAY()
~~~
~~~
TIME()
~~~
