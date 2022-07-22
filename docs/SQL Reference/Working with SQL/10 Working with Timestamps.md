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

### Format

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
~~~
TIME_BUCKET(timestamp, multiple, unit)
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
~~~
generate_series()
~~~

Note that `current_date` and `current_time` support being called without parenthesis.


Recognized date parts and periods and support across various functions:

Part     | DATE_TRUNC | EXTRACT | DATEDIFF | TIME_BUCKET | Notes
-------- | :--------: | :-----: | :------: | :---------: | ----
second   | ✓          | ✓       | ✓        | ✓           |
minute   | ✓          | ✓       | ✓        | ✓           |
hour     | ✓          | ✓       | ✓        | ✓           |
day      | ✓          | ✓       | ✓        | ✓           |
dow      | ✘          | ✓       | ✘        | ✘           | day of week
week     | ✓          | ✓       | ✓        | ✓           | iso week i.e. to monday
month    | ✓          | ✓       | ▲        | ✓           | DATEFIFF unreliable calculating months
quarter  | ✓          | ✓       | ✓        | ✓           |
doy      | ✘          | ✓       | ✘        | ✘           | day of year
year     | ✓          | ✓       | ✓        | ✓           |

The following convenience extraction functions also exist, however use of `EXTRACT` is recommended.

## Implicit Casting

In many situation where a timestamp is expected, if an ISO1806 formatted string is provided, Opteryx will interpret as a timestamp.

## Timezones

Opteryx is opinionated to run in UTC - all instances where the system time is requested, UTC is used.