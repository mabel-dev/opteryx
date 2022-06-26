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
`DATE_TRUNC(period, date)` | Remove parts from a timestamp         | `DATE_TRUNC('year', 2022-06-23) -> '2022-01-01'` 
`DAY(date)`     | Extract day number                                | `DAY(2022-02-06) -> 6`
`HOUR(time)`    | Extract hour from timestamp                       | `HOUR(5:32:43) -> 5`
`MINUTE(time)`  | Extract minute from timestamp                     | `MINUTE(5:32:43) -> 32`
`MONTH(date)`   | Extract month number                              | `MONTH(2022-02-06) -> 2`
`NOW()`         | Current Timestamp                                 | `NOW() -> '2022-02-23 12:37'`
`QUARTER(date)` | Extract quarter of the year                       | `QUARTER(2022-02-06) -> 2`
`TIME()`        | Current Time (UTC)                                | `TIME() -> '12:34:23.2123'`
`SECOND(time)`  | Extract second                                    | `SECOND(5:32:43) -> 43`
`TIMESTAMP(str)` | Convert an [ISO 8601](https://www.iso.org/iso-8601-date-and-time-format.html) format string to a timestamp | `TIMESTAMP('2000-01-01') -> 1 Jan 2000`
`TIMESTAMP(n)`  | Interpret n as seconds since unix epoch           | `TIMESTAMP(946684800) -> 1 Jan 2000`
`TODAY()`       | Current Date                                      | `TODAY() -> '2022-02-23'`
`WEEK(date)`    | Extract ISO week of year number                   | `WEEK(2022-02-06) -> 5`
`YEAR(date)`    | Extract year number                               | `YEAR(202-02-06) -> 2022`


Recognized periods for use with the `DATE_TRUNC` function are: 

- second
- minute
- hour
- day
- week (iso week i.e. to monday)
- month
- quarter
- year

## Other Functions

Function            | Description                                       | Example
------------------- | ------------------------------------------------- | ---------------------------
`BOOLEAN(str)`      | Convert input to a Boolean                        | `BOOLEAN('true') -> True`
`CAST(any AS type)` | Cast any to type, calls `type(any)`               | `CAST(state AS BOOLEAN) -> False`
`COALESCE(args)`    | Return the first item from args which is not None | `CAST(university, high_school) -> 'Olympia High'`  
`GET(list, n)` :fontawesome-solid-asterisk: | Gets the nth element in a list, also `list[n]`    | `GET(names, 2) -> 'Joe'`
`GET(struct, a)` :fontawesome-solid-asterisk: | Gets the element called 'a' from a struct, also `struct[a]` | `GET(dict, 'key') -> 'value'`
`HASH(str)`         | Calculate the [CityHash](https://opensource.googleblog.com/2011/04/introducing-cityhash.html) (64 bit) of a value  | `HASH('hello') -> 'B48BE5A931380CE8'`
`LIST_CONTAINS(list, val)`      | Test if a list field contains a value | `LIST_CONTAINS(letters, '1') -> false`
`LIST_CONTAINS_ANY(list, vals)` | Test if a list field contains any of a list of values | `LIST_CONTAINS_ANY(letters, ('1', 'a')) -> true`
`LIST_CONTAINS_ALL(list, vals)` | Test is a list field contains all of a list of values | `LIST_CONTAINS_ALL(letters, ('1', 'a')) -> false`
`MD5(str)`          | Calculate the MD5 hash of a value                 | `MD5('hello') -> '5d41402abc4b2a76b9719d911017c592'`
`RANDOM()`          | Random number between 0.000 and 0.999             | `RANDOM() -> 0.234`
`SEARCH(list, val)` :fontawesome-solid-asterisk: | Return True if val is an item in list             | `SEARCH(names, 'John') -> TRUE`
`SEARCH(struct, val)` :fontawesome-solid-asterisk: | Return True if any of the keys or values in struct is val | `SEARCH(dict, 'key') -> TRUE`
`UNNEST(list)`      | Create a virtual table with a row for each element in the LIST | `UNNEST((TRUE,FALSE)) AS Booleans` 
`VERSION()`         | Return the version of Opteryx                     | `VERSION() -> 0.1.0`

