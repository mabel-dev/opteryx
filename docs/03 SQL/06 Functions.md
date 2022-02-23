# SQL Functions

## Numeric Functions

Function        | Description                                       | Example
--------------- | ------------------------------------------------- | ---------------------------
`ABS(n)`        | The absolute value on n                           | ABS(-8) -> 8   
`CEIL(n)`       | Round the number up                               | CEIL(2.3) -> 3            
`FLOOR(n)`      | Round the number down                             | FLOOR(3.9) -> 3 
`NUMERIC(n)`    | Convert n to a floating point number              | NUMERIC('2') -> 2.0
`ROUND(n)`      | Round to nearest whole number                     | ROUND(0.2) -> 0
`TRUNC(n)`      | Remove the fractional parts                       | TRUNC(5.5) -> 5

## Text Functions

Functions for examining and manipulating string values. 

Function        | Description                                       | Example
--------------- | ------------------------------------------------- | ---------------------------
`LEFT(str, n)`  | Extract the left-most n characters                | LEFT('hello', 2) -> 'he'
`LENGTH(str)`   | Number of characters in string                    | LENGTH('hello') -> 5
`LOWER(str)`    | Convert string to lower case                      | LOWER('Hello') -> 'hello'
`RIGHT(str, n)` | Extract the right-most n characters               | RIGHT('hello', 2) -> 'lo'
`TRIM(str)`     | Removes any spaces from either side of the string | TRIM('  hello  ') -> 'hello'
`UPPER(str)`    | Convert string to upper case                      | UPPER('Hello') -> 'HELLO'
`VARCHAR(any)`  | Convert value to a string                         | VARCHAR(22) -> '22'

## Date Functions

Functions for examining and manipulating date values. 

Function        | Description                                       | Example
--------------- | ------------------------------------------------- | ---------------------------
`DAY(date)`     | Extract day number                                | DAY(2022-02-06) -> 6
`HOUR(time)`    | Extract hour from timestamp                       | HOUR(5:32:43) -> 5
`MINUTE(time)`  | Extract minute from timestamp                     | MINUTE(5:32:43) -> 32
`MONTH(date)`   | Extract month number                              | MONTH(2022-02-06) -> 2
`NOW()`         | Current Timestamp                                 | NOW() -> '2022-02-23 12:37'
`TIME()`        | Current Time (UTC)                                | TIME() -> '12:34:23.2123'
`SECOND(time)`  | Extract second                                    | SECOND(5:32:43) -> 43
`TIMESTAMP(str)` | Convert an [ISO 8601](https://www.iso.org/iso-8601-date-and-time-format.html) format string to a timestamp | TIMESTAMP('2000-01-01') -> 1 Jan 2000
`TIMESTAMP(n)`  | Interpret n as seconds since unix epoch           | TIMESTAMP(946684800) -> 1 Jan 2000
`TODAY()`       | Current Date                                      | TODAY() -> '2022-02-23'
`WEEK(date)`    | Extract ISO week of year number                   | WEEK(2022-02-06) -> 5
`YEAR(date)`    | Extract year number                               | YEAR(202-02-06) -> 2022


## Other Functions

Function         | Description                                       | Example
---------------- | ------------------------------------------------- | ---------------------------
`BOOLEAN(str)`   | Convert input to a Boolean                        | BOOLEAN('true') -> True 
`GET(struct, a)` | Gets the element called 'a' from a struct         | GET(dict, 'key') -> 'value
`GET(list, n)`   | Gets the nth element in a list                    | GET(names, 2) -> 'Joe'
`GET(str, n)`    | Gets the nth element in a string                  | GET('hello', 2) -> 'e'
`RANDOM()`       | Random number between 0.000 and 0.999             | RANDOM() -> 0.234
`HASH(str)`      | Calculate the [CityHash](https://opensource.googleblog.com/2011/04/introducing-cityhash.html) (64 bit) of a value  | HASH('') ->
`MD5(str)`       | Calculate the MD5 hash of a value                 | MD5('') ->
