# Functions

## Numeric Functions

`ABS` (**x**: _numeric_) → _numeric_  
&emsp;Returns the absolute value of **x**.    
&emsp;Alias of `ABSOLUTE`(_numeric_).  

`CEIL` (**x**: _numeric_) → _numeric_   
&emsp;Returns **x** rounded up to the nearest integer.    
&emsp;Alias of `CEILING`(_numeric_).  

`FLOOR` (**x**: _numeric_) → _numeric_   
&emsp;Returns **x** rounded down to the nearest integer.   

`PI` () → _numeric_   
&emsp;Returns the constant Pi.  

`ROUND` (**x**: _numeric_) → _numeric_     
&emsp;Returns **x** rounded to the nearest integer. 

`ROUND` (**x**: _numeric_, **places**: _numeric_) → _numeric_     
&emsp;Returns **x** rounded to **places** decimal places.

`TRUNC` (**x**: _numeric_) → _numeric_    
&emsp;Returns **x** rounded to integer by dropping digits after decimal point.    
&emsp;Alias of `TRUNCATE`(_numeric_).  

## Text Functions

Functions for examining and manipulating string values. 

`GET` (**str**: _varchar_, **index**: _numeric_) → _varchar_   
&emsp;Return the **index**th character from **str**.   

`LEFT` (**str**: _varchar_, **n**: _numeric_) → _varchar_    
&emsp;Extract the left-most **n** characters of **str**.  

`LEN` (**str**: _varchar_) → _numeric_   
&emsp;Returns the length of **str** in characters.    
&emsp;Alias of `LENGTH`(_varchar_)

`LOWER` (**str**: _varchar_) → _varchar_   
&emsp;Converts **str** to lowercase.

`RIGHT` (**str**: _varchar_, **n**: _numeric_) → _varchar_    
&emsp;Extract the right-most **n** characters of **str**.   

`SEARCH` (_varchar_, **value**: _varchar_) → _boolean_    
&emsp;Return True if the string contains value.   
&emsp;`SEARCH('hello', 'lo') -> TRUE`

`TRIM` (**str**: _varchar_) → _varchar_   
&emsp;Removes leading and trailing whitespace from **str**.  

`UPPER` (**str**: _varchar_) → _varchar_   
&emsp;Converts str to uppercase.  

## Date Functions

Functions for examining and manipulating date values. 

`current_date` → _timestamp_      
&emsp;Current Date, also `TODAY`(). Note `current_date` does not require parenthesis.  
&emsp;`CURRENT_DATE -> '2022-02-23'`

`current_time` → _timestamp_      
&emsp;Current Timestamp, also `NOW`(). Note `current_time` does not require parenthesis.  
&emsp;`CURRENT_TIME -> '2022-02-23 12:37'`

`DATE` (_timestamp_) → _timestamp_      
&emsp;Extract the date part of a timestamp.   
&emsp;`DATE(2022-02-06 11:37) -> '2022-02-06 00:00'`

`DATE_FORMAT` (_timestamp_, **format**: _varchar_) → _varchar_      
&emsp;Formats `timestamp` as a string using `format`.   
&emsp;`DATE_FORMAT('2022-07-07', '%Y') -> '2022'`    

`DATE_TRUNC` (**part**: _varchar_, _timestamp_) → _varchar_      
&emsp;Remove parts from a timestamp.  
&emsp;`DATE_TRUNC('year', 2022-06-23) -> '2022-01-01'`

`DATEDIFF` (**part**: _varchar_, **start**: _timestamp_, **end**: _timestamp_) → _numeric_      
&emsp;Calculate the difference between the start and end timestamps in a given unit  
&emsp;`DATEDIFF('hours', '1969-07-16 13:32', '1969-07-24 16:50') -> 195`

`EXTRACT` (_part_ FROM _timestamp_) → _numeric_     
&emsp;Extract a part of a timestamp, also `DATE_PART`(part: _varchar_, _timestamp_)  
&emsp;`EXTRACT(year FROM 2022-01-01) -> 2022`

`TIME` () → _timestamp_     
&emsp;Current Time (UTC).   
&emsp;`TIME() -> '12:34:23.2123'`

`TIMESTAMP` (_varchar_) → _timestamp_        
&emsp;Convert an [ISO 8601](https://www.iso.org/iso-8601-date-and-time-format.html) format string to a timestamp.  
&emsp;`TIMESTAMP('2000-01-01') -> 1 Jan 2000`

`TIMESTAMP` (_numeric_) → _timestamp_     
&emsp;Interpret n as seconds since unix epoch.   
&emsp;`TIMESTAMP(946684800) -> 1 Jan 2000`

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
&emsp;Convert input to a Boolean   
&emsp;`BOOLEAN('true') -> True`

`CAST` (_any_ AS _type_) → _type_   
&emsp;Cast a value to type, calls `type(any)`    
&emsp;`CAST(state AS BOOLEAN) -> False`

`COALESCE` (_args_) → _input type_   
&emsp;Return the first item from args which is not None   
&emsp;`CAST(university, high_school) -> 'Olympia High'`  

`GENERATE_SERIES` (**stop**: _numeric_) → _list_<_numeric_>       
&emsp;Generate a series between 1 and 'stop', with a step of 1    
&emsp;`GENERATE_SERIES(2) -> (1,2)`   

`GENERATE_SERIES` (**start**: _numeric_, **stop**: _numeric_) → _list_<_numeric_>       
&emsp;`NUMERIC` series between 'start' and 'stop', with a step of 1
&emsp;`GENERATE_SERIES(2,4) -> (2,3,4)`

`GENERATE_SERIES` (**start**: _numeric_, **stop**: _numeric_, **step**: _numeric_) → _list_<_numeric_>       
&emsp;`NUMERIC` series between 'start' and 'stop', with an explicit step size.
&emsp;`GENERATE_SERIES(2, 6, 2) -> (2,4,6)`

`GENERATE_SERIES` (**start**: _timestamp_, **stop**: _timestamp_, _interval_) → _list_<_timestamp_>       
&emsp;`TIMESTAMP` series between 'start' and 'stop', with a given interval    
&emsp;`GENERATE_SERIES('2022-01-01', '2023-12-31, '1y') -> ('2022-01-01')`

`GENERATE_SERIES` (**cidr**: _varchar_) → _list_<_varchar_>       
&emsp;Set of IP addresses from a given CIDR   
&emsp;`GENERATE_SERIES('192.168.1.1/32') -> ('192.168.1.1')`

`GET(list, n)`
&emsp;Gets the nth element in a list, also `list[n]`
&emsp;`GET(names, 2) -> 'Joe'`

`GET(struct, a)`
&emsp;Gets the element called 'a' from a struct, also `struct[a]`
&emsp;`GET(dict, 'key') -> 'value'`

`HASH(str)`
&emsp;Calculate the [CityHash](https://opensource.googleblog.com/2011/04/introducing-cityhash.html) (64 bit) of a value
&emsp;`HASH('hello') -> 'B48BE5A931380CE8'`

`LIST_CONTAINS(list, val)`
&emsp;Test if a list field contains a value
&emsp;`LIST_CONTAINS(letters, '1') -> False`

`LIST_CONTAINS_ANY(list, vals)`
&emsp;Test if a list field contains any of a list of values
&emsp;`LIST_CONTAINS_ANY(letters, ('1', 'a')) -> True`

`LIST_CONTAINS_ALL(list, vals)`
&emsp;Test is a list field contains all of a list of values
&emsp;`LIST_CONTAINS_ALL(letters, ('1', 'a')) -> False`

`MD5(str)`
&emsp;Calculate the MD5 hash of a value
&emsp;`MD5('hello') -> '5d41402abc4b2a76b9719d911017c592'`

`RANDOM()`
&emsp;Random number between 0.000 and 0.999
&emsp;`RANDOM() -> 0.234`

`SEARCH(list, val)`
&emsp;Return True if val is an item in list
&emsp;`SEARCH(names, 'John') -> True`

`SEARCH(struct, val)`
&emsp;Return True if any of the values in struct is val
&emsp;`SEARCH(dict, 'key') -> True`

`UNNEST(list)`
&emsp;Create a virtual table with a row for each element in the LIST
&emsp;`UNNEST((TRUE,FALSE)) AS Booleans` 

`VERSION()`
&emsp;Return the version of Opteryx
&emsp;`VERSION() -> 0.1.0`

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




`NUMERIC` (_numeric_) → _numeric_      
&emsp;Convert input to a floating point number. 


`VARCHAR` (_any_) → _varchar_   
&emsp;Convert value to a string, also `STRING` (_any_).   
&emsp;`VARCHAR(22) -> '22'`
