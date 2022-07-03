# Functions

!!! note
    :fontawesome-solid-asterisk: indicates a function has more than one entry on this page.

## Numeric Functions

Function        | Description                                       | Example
--------------- | ------------------------------------------------- | ---------------------------
`ABS(n)`        | The absolute value on n, also `ABSOLUTE`          | `ABS(-8) -> 8`   
`CEIL(n)`       | Round the number up, also `CEILING`               | `CEIL(2.3) -> 3`        
`FLOOR(n)`      | Round the number down                             | `FLOOR(3.9) -> 3` 
`NUMERIC(n)`    | Convert n to a floating point number              | `NUMERIC('2') -> 2.0`
`ROUND(n)`      | Round to nearest whole number                     | `ROUND(0.2) -> 0`
`TRUNC(n)`      | Remove the fractional parts, also `TRUNCATE`      | `TRUNC(5.5) -> 5`

## Text Functions

Functions for examining and manipulating string values. 

Function        | Description                                       | Example
--------------- | ------------------------------------------------- | ---------------------------
`GET(str, n)` :fontawesome-solid-asterisk:  | Gets the nth element in a string, also `str[n]`   | `GET('hello', 2) -> 'e'`
`LEFT(str, n)`  | Extract the left-most n characters                | `LEFT('hello', 2) -> 'he'`
`LEN(str)`      | Number of characters in string, also `LENGTH`     | `LEN('hello') -> 5`
`LOWER(str)`    | Convert string to lower case                      | `LOWER('Hello') -> 'hello'`
`RIGHT(str, n)` | Extract the right-most n characters               | `RIGHT('hello', 2) -> 'lo'`
`SEARCH(str, val)` :fontawesome-solid-asterisk: | Return True if str contains val                | `SEARCH('hello', 'lo') -> TRUE`
`STRING(any)`   | Alias of `VARCHAR()`                              | `STRING(22) -> '22'`
`TRIM(str)`     | Removes any spaces from either side of the string | `TRIM('  hello  ') -> 'hello'`
`UPPER(str)`    | Convert string to upper case                      | `UPPER('Hello') -> 'HELLO'`
`VARCHAR(any)`  | Convert value to a string                         | `VARCHAR(22) -> '22'`

## Date Functions

Functions for examining and manipulating date values. 

Function        | Description                                       | Example
--------------- | ------------------------------------------------- | ---------------------------
`DATE(date)`    | Extract the date part                             | `DATE(2022-02-06 11:37) -> '2022-02-06 00:00'`
`DATE_TRUNC(period, date)` | Remove parts from a timestamp          | `DATE_TRUNC('year', 2022-06-23) -> '2022-01-01'` 
`DATEPART(part, date)` | Functional representation of `EXTRACT`     | `DATEPART('year', 2022-01-01) -> 2022`
`EXTRACT(part FROM date)` | Extract a part of a timestamp           | `EXTRACT(year FROM 2022-01-01) -> 2022`
`NOW()`         | Current Timestamp                                 | `NOW() -> '2022-02-23 12:37'`
`TIME()`        | Current Time (UTC)                                | `TIME() -> '12:34:23.2123'`
`TIMESTAMP(str)` :fontawesome-solid-asterisk: | Convert an [ISO 8601](https://www.iso.org/iso-8601-date-and-time-format.html) format string to a timestamp | `TIMESTAMP('2000-01-01') -> 1 Jan 2000`
`TIMESTAMP(n)` :fontawesome-solid-asterisk: | Interpret n as seconds since unix epoch           | `TIMESTAMP(946684800) -> 1 Jan 2000`
`TODAY()`       | Current Date                                      | `TODAY() -> '2022-02-23'`

Recognized date parts and periods and support across various functions:

Part     | DATE_TRUNC                | EXTRACT                   | Notes
-------- | ------------------------- | ------------------------- | -------------
second   | :fontawesome-solid-check: | :fontawesome-solid-check: |
minute   | :fontawesome-solid-check: | :fontawesome-solid-check: |
hour     | :fontawesome-solid-check: | :fontawesome-solid-check: |
day      | :fontawesome-solid-check: | :fontawesome-solid-check: |
dow      | :fontawesome-solid-xmark: | :fontawesome-solid-check: | day of week
week     | :fontawesome-solid-check: | :fontawesome-solid-check: | iso week i.e. to monday
month    | :fontawesome-solid-check: | :fontawesome-solid-check: |
quarter  | :fontawesome-solid-check: | :fontawesome-solid-check: |
doy      | :fontawesome-solid-xmark: | :fontawesome-solid-check: | day of year
year     | :fontawesome-solid-check: | :fontawesome-solid-check: |

The following functions exist, however use of `EXTRACT` is recommended.

Function        | Description                                       | Example
--------------- | ------------------------------------------------- | ---------------------------
`DAY(date)`     | Extract day number                                | `DAY(2022-02-06) -> 6`
`HOUR(time)`    | Extract hour from timestamp                       | `HOUR(5:32:43) -> 5`
`MINUTE(time)`  | Extract minute from timestamp                     | `MINUTE(5:32:43) -> 32`
`MONTH(date)`   | Extract month number                              | `MONTH(2022-02-06) -> 2`
`QUARTER(date)` | Extract quarter of the year                       | `QUARTER(2022-02-06) -> 2`
`SECOND(time)`  | Extract second                                    | `SECOND(5:32:43) -> 43`
`WEEK(date)`    | Extract ISO week of year number                   | `WEEK(2022-02-06) -> 5`
`YEAR(date)`    | Extract year number                               | `YEAR(202-02-06) -> 2022`

## Other Functions

Function            | Description                                       | Example
------------------- | ------------------------------------------------- | ---------------------------
`BOOLEAN(str)`      | Convert input to a Boolean                        | `BOOLEAN('true') -> True`
`CAST(any AS type)` | Cast any to type, calls `type(any)`               | `CAST(state AS BOOLEAN) -> False`
`COALESCE(args)`    | Return the first item from args which is not None | `CAST(university, high_school) -> 'Olympia High'`  
`GENERATE_SERIES(stop)`              | `NUMERIC` series between 1 and 'stop', with a step of 1                  | `GENERATE_SERIES(2) -> (1,2)`   
`GENERATE_SERIES(start, stop)`       | `NUMERIC` series between 'start' and 'stop', with a step of 1            | `GENERATE_SERIES(2,4) -> (2,3,4)`
`GENERATE_SERIES(start, stop, step)` | `NUMERIC` series between 'start' and 'stop', with an explicit step size  | `GENERATE_SERIES(2, 6, 2) -> (2,4,6)`
`GENERATE_SERIES(start, stop, interval)` | `TIMESTAMP` series between 'start' and 'stop', with a given interval | `GENERATE_SERIES('2022-01-01', '2023-12-31, '1y') -> ('2022-01-01')`
`GENERATE_SERIES(cidr)`              | Set of IP addresses from a given CIDR | `GENERATE_SERIES('192.168.1.1/32') -> ('192.168.1.1')`
`GET(list, n)` :fontawesome-solid-asterisk: | Gets the nth element in a list, also `list[n]`    | `GET(names, 2) -> 'Joe'`
`GET(struct, a)` :fontawesome-solid-asterisk: | Gets the element called 'a' from a struct, also `struct[a]` | `GET(dict, 'key') -> 'value'`
`HASH(str)`         | Calculate the [CityHash](https://opensource.googleblog.com/2011/04/introducing-cityhash.html) (64 bit) of a value  | `HASH('hello') -> 'B48BE5A931380CE8'`
`LIST_CONTAINS(list, val)`      | Test if a list field contains a value | `LIST_CONTAINS(letters, '1') -> False`
`LIST_CONTAINS_ANY(list, vals)` | Test if a list field contains any of a list of values | `LIST_CONTAINS_ANY(letters, ('1', 'a')) -> True`
`LIST_CONTAINS_ALL(list, vals)` | Test is a list field contains all of a list of values | `LIST_CONTAINS_ALL(letters, ('1', 'a')) -> False`
`MD5(str)`          | Calculate the MD5 hash of a value                 | `MD5('hello') -> '5d41402abc4b2a76b9719d911017c592'`
`RANDOM()`          | Random number between 0.000 and 0.999             | `RANDOM() -> 0.234`
`SEARCH(list, val)` :fontawesome-solid-asterisk: | Return True if val is an item in list             | `SEARCH(names, 'John') -> True`
`SEARCH(struct, val)` :fontawesome-solid-asterisk: | Return True if any of the values in struct is val | `SEARCH(dict, 'key') -> True`
`UNNEST(list)`      | Create a virtual table with a row for each element in the LIST | `UNNEST((TRUE,FALSE)) AS Booleans` 
`VERSION()`         | Return the version of Opteryx                     | `VERSION() -> 0.1.0`

Recognized interval parts for the `GENERATE_SERIES` function are:

Period  | Symbol
------- | -----:
Years   | y
Months  | mo
Weeks   | w
Days    | d
Hours   | h
Minutes | m
Seconds | s
