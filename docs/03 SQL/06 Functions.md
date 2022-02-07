# SQL Functions

## Numeric Functions

Function        | Description                                       | Example
--------------- | ------------------------------------------------- | ---------------------------
`ABS(n)`        | The absolute value on n                           | ABS(-8) -> 8   
`CEIL(n)`       | Round the number up                               | CEIL(2.3) -> 3            
`FLOOR(n)`      | Round the number down                             | FLOOR(3.9) -> 3 
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

## Date Functions

None currently implemented

## Other Functions

Function        | Description                                       | Example
--------------- | ------------------------------------------------- | ---------------------------
`HASH(str)`     | Calculate the CityHash of a value                 | HASH('')
`MD5(str)`      | Calculate the MD5 hash of a value                 | MD5('')