# Functions

## Numeric Functions

`ABS` (_numeric_) → _numeric_  
&nbsp;&nbsp;&nbsp;&nbsp;The absolute value on n, also `ABSOLUTE`(_numeric_).  
&nbsp;&nbsp;&nbsp;&nbsp;`ABS(-8) -> 8`   

`CEIL` (_numeric_) → _numeric_   
&nbsp;&nbsp;&nbsp;&nbsp;Round the number up, also `CEILING`(_numeric_).  
&nbsp;&nbsp;&nbsp;&nbsp;`CEIL(2.3) -> 3`        

`FLOOR` (_numeric_) → _numeric_   
&nbsp;&nbsp;&nbsp;&nbsp;Round the number down.  
&nbsp;&nbsp;&nbsp;&nbsp;`FLOOR(3.9) -> 3` 

`NUMERIC` (_numeric_) → _numeric_      
&nbsp;&nbsp;&nbsp;&nbsp;Convert input to a floating point number.  
&nbsp;&nbsp;&nbsp;&nbsp;`NUMERIC('2') -> 2.0`

`ROUND` (_numeric_) → _numeric_     
&nbsp;&nbsp;&nbsp;&nbsp;Round to nearest whole number.  
&nbsp;&nbsp;&nbsp;&nbsp;`ROUND(0.2) -> 0`

`TRUNC` (_numeric_) → _numeric_    
&nbsp;&nbsp;&nbsp;&nbsp;Remove the fractional parts, also `TRUNCATE`(_numeric_).  
&nbsp;&nbsp;&nbsp;&nbsp;`TRUNC(5.5) -> 5`

## Text Functions

Functions for examining and manipulating string values. 

`GET` (_varchar_, _numeric_) → _varchar_   
&nbsp;&nbsp;&nbsp;&nbsp;Gets the specified element of a string.   
&nbsp;&nbsp;&nbsp;&nbsp;`GET('hello', 2) -> 'e'`

`LEFT` (_varchar_, _numeric_) → _varchar_    
&nbsp;&nbsp;&nbsp;&nbsp; Extract the left-most characters  
&nbsp;&nbsp;&nbsp;&nbsp;`LEFT('hello', 2) -> 'he'`

`LEN` (_varchar_) → _numeric_   
&nbsp;&nbsp;&nbsp;&nbsp;Number of characters in string, also `LENGTH`(_varchar_).  
&nbsp;&nbsp;&nbsp;&nbsp;`LEN('hello') -> 5`

`LOWER` (_varchar_) → _varchar_   
&nbsp;&nbsp;&nbsp;&nbsp;Convert string to lower case.
&nbsp;&nbsp;&nbsp;&nbsp;`LOWER('Hello') -> 'hello'`

`RIGHT` (_varchar_, _numeric_) → _varchar_    
&nbsp;&nbsp;&nbsp;&nbsp;Extract the right-most characters.  
&nbsp;&nbsp;&nbsp;&nbsp;`RIGHT('hello', 2) -> 'lo'`

`SEARCH` (_varchar_, **value**: _varchar_) → _boolean_    
&nbsp;&nbsp;&nbsp;&nbsp;Return True if the string contains value.   
&nbsp;&nbsp;&nbsp;&nbsp;`SEARCH('hello', 'lo') -> TRUE`

`TRIM` (_varchar_) → _varchar_   
&nbsp;&nbsp;&nbsp;&nbsp;Removes any spaces from either side of the string.  
&nbsp;&nbsp;&nbsp;&nbsp;`TRIM('  hello  ') -> 'hello'`

`UPPER` (_varchar_) → _varchar_   
&nbsp;&nbsp;&nbsp;&nbsp;Convert string to upper case.  
&nbsp;&nbsp;&nbsp;&nbsp;`UPPER('Hello') -> 'HELLO'`

`VARCHAR` (_any_) → _varchar_   
&nbsp;&nbsp;&nbsp;&nbsp;Convert value to a string, also `STRING` (_any_).   
&nbsp;&nbsp;&nbsp;&nbsp;`VARCHAR(22) -> '22'`

## Date Functions

Functions for examining and manipulating date values. 

`current_date` → _timestamp_      
&nbsp;&nbsp;&nbsp;&nbsp;Current Date, also `TODAY`(). Note `current_date` does not require parenthesis.  
&nbsp;&nbsp;&nbsp;&nbsp;`CURRENT_DATE -> '2022-02-23'`

`current_time` → _timestamp_      
&nbsp;&nbsp;&nbsp;&nbsp;Current Timestamp, also `NOW`(). Note `current_time` does not require parenthesis.  
&nbsp;&nbsp;&nbsp;&nbsp;`CURRENT_TIME -> '2022-02-23 12:37'`

`DATE` (_timestamp_) → _timestamp_      
&nbsp;&nbsp;&nbsp;&nbsp;Extract the date part of a timestamp.   
&nbsp;&nbsp;&nbsp;&nbsp;`DATE(2022-02-06 11:37) -> '2022-02-06 00:00'`

`DATE_FORMAT` (_timestamp_, **format**: _varchar_) → _varchar_      
&nbsp;&nbsp;&nbsp;&nbsp;Formats `timestamp` as a string using `format`.   
&nbsp;&nbsp;&nbsp;&nbsp;`DATE_FORMAT('2022-07-07', '%Y') -> '2022'`    

`DATE_TRUNC` (**part**: _varchar_, _timestamp_) → _varchar_      
&nbsp;&nbsp;&nbsp;&nbsp;Remove parts from a timestamp.  
&nbsp;&nbsp;&nbsp;&nbsp;`DATE_TRUNC('year', 2022-06-23) -> '2022-01-01'`

`DATEDIFF` (**part**: _varchar_, **start**: _timestamp_, **end**: _timestamp_) → _numeric_      
&nbsp;&nbsp;&nbsp;&nbsp;Calculate the difference between the start and end timestamps in a given unit  
&nbsp;&nbsp;&nbsp;&nbsp;`DATEDIFF('hours', '1969-07-16 13:32', '1969-07-24 16:50') -> 195`

`EXTRACT` (_part_ FROM _timestamp_) → _numeric_     
&nbsp;&nbsp;&nbsp;&nbsp;Extract a part of a timestamp, also `DATE_PART`(part: _varchar_, _timestamp_)  
&nbsp;&nbsp;&nbsp;&nbsp;`EXTRACT(year FROM 2022-01-01) -> 2022`

`TIME` () → _timestamp_     
&nbsp;&nbsp;&nbsp;&nbsp;Current Time (UTC).   
&nbsp;&nbsp;&nbsp;&nbsp;`TIME() -> '12:34:23.2123'`

`TIMESTAMP` (_varchar_) → _timestamp_        
&nbsp;&nbsp;&nbsp;&nbsp;Convert an [ISO 8601](https://www.iso.org/iso-8601-date-and-time-format.html) format string to a timestamp.  
&nbsp;&nbsp;&nbsp;&nbsp;`TIMESTAMP('2000-01-01') -> 1 Jan 2000`

`TIMESTAMP` (_numeric_) → _timestamp_     
&nbsp;&nbsp;&nbsp;&nbsp;Interpret n as seconds since unix epoch.   
&nbsp;&nbsp;&nbsp;&nbsp;`TIMESTAMP(946684800) -> 1 Jan 2000`

Recognized date parts and periods and support across various functions:

Part     | DATE_TRUNC                | EXTRACT                   | DATEDIFF                  | Notes
-------- | ------------------------- | ------------------------- | ------------------------- | ----
second   | :fontawesome-solid-check: | :fontawesome-solid-check: | :fontawesome-solid-check: |
minute   | :fontawesome-solid-check: | :fontawesome-solid-check: | :fontawesome-solid-check: |
hour     | :fontawesome-solid-check: | :fontawesome-solid-check: | :fontawesome-solid-check: |
day      | :fontawesome-solid-check: | :fontawesome-solid-check: | :fontawesome-solid-check: |
dow      | :fontawesome-solid-xmark: | :fontawesome-solid-check: | :fontawesome-solid-xmark: | day of week
week     | :fontawesome-solid-check: | :fontawesome-solid-check: | :fontawesome-solid-check: | iso week i.e. to monday
month    | :fontawesome-solid-check: | :fontawesome-solid-check: | :fontawesome-solid-check: | DATEFIFF unreliable calculating months
quarter  | :fontawesome-solid-check: | :fontawesome-solid-check: | :fontawesome-solid-check: |
doy      | :fontawesome-solid-xmark: | :fontawesome-solid-check: | :fontawesome-solid-xmark: | day of year
year     | :fontawesome-solid-check: | :fontawesome-solid-check: | :fontawesome-solid-check: |

The following functions also exist, however use of `EXTRACT` is recommended.

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

!!! note
    Date functions presently require a TIMESTAMP column or a TIMESTAMP literal and cannot be used with the outputs of function calls, for example `DATEDIFF('year', birth_date, TODAY())` will return an error.

## Other Functions

`BOOLEAN` (_any_) → _boolean_        
&nbsp;&nbsp;&nbsp;&nbsp;Convert input to a Boolean   
&nbsp;&nbsp;&nbsp;&nbsp;`BOOLEAN('true') -> True`

`CAST` (_any_ AS _type_) → _type_   
&nbsp;&nbsp;&nbsp;&nbsp;Cast a value to type, calls `type(any)`    
&nbsp;&nbsp;&nbsp;&nbsp;`CAST(state AS BOOLEAN) -> False`

`COALESCE` (_args_) → _input type_   
&nbsp;&nbsp;&nbsp;&nbsp;Return the first item from args which is not None   
&nbsp;&nbsp;&nbsp;&nbsp;`CAST(university, high_school) -> 'Olympia High'`  

`GENERATE_SERIES` (**stop**: _numeric_) → _list_<_numeric_>       
&nbsp;&nbsp;&nbsp;&nbsp;Generate a series between 1 and 'stop', with a step of 1    
&nbsp;&nbsp;&nbsp;&nbsp;`GENERATE_SERIES(2) -> (1,2)`   

`GENERATE_SERIES` (**start**: _numeric_, **stop**: _numeric_) → _list_<_numeric_>       
&nbsp;&nbsp;&nbsp;&nbsp;`NUMERIC` series between 'start' and 'stop', with a step of 1
&nbsp;&nbsp;&nbsp;&nbsp;`GENERATE_SERIES(2,4) -> (2,3,4)`

`GENERATE_SERIES` (**start**: _numeric_, **stop**: _numeric_, **step**: _numeric_) → _list_<_numeric_>       
&nbsp;&nbsp;&nbsp;&nbsp;`NUMERIC` series between 'start' and 'stop', with an explicit step size.
&nbsp;&nbsp;&nbsp;&nbsp;`GENERATE_SERIES(2, 6, 2) -> (2,4,6)`

`GENERATE_SERIES` (**start**: _timestamp_, **stop**: _timestamp_, _interval_) → _list_<_timestamp_>       
&nbsp;&nbsp;&nbsp;&nbsp;`TIMESTAMP` series between 'start' and 'stop', with a given interval    
&nbsp;&nbsp;&nbsp;&nbsp;`GENERATE_SERIES('2022-01-01', '2023-12-31, '1y') -> ('2022-01-01')`

`GENERATE_SERIES` (**cidr**: _varchar_) → _list_<_varchar_>       
&nbsp;&nbsp;&nbsp;&nbsp;Set of IP addresses from a given CIDR   
&nbsp;&nbsp;&nbsp;&nbsp;`GENERATE_SERIES('192.168.1.1/32') -> ('192.168.1.1')`

`GET(list, n)`
&nbsp;&nbsp;&nbsp;&nbsp;Gets the nth element in a list, also `list[n]`
&nbsp;&nbsp;&nbsp;&nbsp;`GET(names, 2) -> 'Joe'`

`GET(struct, a)`
&nbsp;&nbsp;&nbsp;&nbsp;Gets the element called 'a' from a struct, also `struct[a]`
&nbsp;&nbsp;&nbsp;&nbsp;`GET(dict, 'key') -> 'value'`

`HASH(str)`
&nbsp;&nbsp;&nbsp;&nbsp;Calculate the [CityHash](https://opensource.googleblog.com/2011/04/introducing-cityhash.html) (64 bit) of a value
&nbsp;&nbsp;&nbsp;&nbsp;`HASH('hello') -> 'B48BE5A931380CE8'`

`LIST_CONTAINS(list, val)`
&nbsp;&nbsp;&nbsp;&nbsp;Test if a list field contains a value
&nbsp;&nbsp;&nbsp;&nbsp;`LIST_CONTAINS(letters, '1') -> False`

`LIST_CONTAINS_ANY(list, vals)`
&nbsp;&nbsp;&nbsp;&nbsp;Test if a list field contains any of a list of values
&nbsp;&nbsp;&nbsp;&nbsp;`LIST_CONTAINS_ANY(letters, ('1', 'a')) -> True`

`LIST_CONTAINS_ALL(list, vals)`
&nbsp;&nbsp;&nbsp;&nbsp;Test is a list field contains all of a list of values
&nbsp;&nbsp;&nbsp;&nbsp;`LIST_CONTAINS_ALL(letters, ('1', 'a')) -> False`

`MD5(str)`
&nbsp;&nbsp;&nbsp;&nbsp;Calculate the MD5 hash of a value
&nbsp;&nbsp;&nbsp;&nbsp;`MD5('hello') -> '5d41402abc4b2a76b9719d911017c592'`

`RANDOM()`
&nbsp;&nbsp;&nbsp;&nbsp;Random number between 0.000 and 0.999
&nbsp;&nbsp;&nbsp;&nbsp;`RANDOM() -> 0.234`

`SEARCH(list, val)`
&nbsp;&nbsp;&nbsp;&nbsp;Return True if val is an item in list
&nbsp;&nbsp;&nbsp;&nbsp;`SEARCH(names, 'John') -> True`

`SEARCH(struct, val)`
&nbsp;&nbsp;&nbsp;&nbsp;Return True if any of the values in struct is val
&nbsp;&nbsp;&nbsp;&nbsp;`SEARCH(dict, 'key') -> True`

`UNNEST(list)`
&nbsp;&nbsp;&nbsp;&nbsp;Create a virtual table with a row for each element in the LIST
&nbsp;&nbsp;&nbsp;&nbsp;`UNNEST((TRUE,FALSE)) AS Booleans` 

`VERSION()`
&nbsp;&nbsp;&nbsp;&nbsp;Return the version of Opteryx
&nbsp;&nbsp;&nbsp;&nbsp;`VERSION() -> 0.1.0`

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
