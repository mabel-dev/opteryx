# Functions

Definitions noted with a ♫ accept different input arguments.

## List Functions

For more details, see [Working with Lists](https://mabel-dev.github.io/opteryx/SQL%20Reference/Working%20with%20SQL/20%20Working%20with%20Lists/).

**array**: _list_`[`**index**: _numeric_`]` → **value** ♫  
&emsp;Return the **index**th element from **array**. 

`GET` (**array**: _list_, **index**: _numeric_) → **value** ♫   
&emsp;Alias of **array**`[`**index**`]`.  

`LEN` (**array**: _list_) → _numeric_ ♫   
&emsp;Alias of `LENGTH`(**array**).

`LENGTH` (**array**: _list_) → _numeric_ ♫   
&emsp;Returns the number of elements in **array**.

`LIST_CONTAINS` (**array**: _list_, **value**) → _boolean_     
&emsp;Return `true` if **array** contains **value**. See also `SEARCH`(**array**, **value**).

`LIST_CONTAINS_ANY` (**array**: _list_, **values**: _list_) → _boolean_       
&emsp;Return `true` if **array** contains any elements in **values**.

`LIST_CONTAINS_ALL` (**array**: _list_, **values**: _list_) → _boolean_            
&emsp;Return `true` if **array** contains all of elements in **values**.

`SEARCH` (**array**: _list_, **value**) → _boolean_ ♫  
&emsp;Return `true` if **array** contains **value**. 

## Numeric Functions

`ABS` (**x**: _numeric_) → _numeric_   
&emsp;Alias of `ABSOLUTE`(**x**).  

`ABSOLUTE` (**x**: _numeric_) → _numeric_   
&emsp;Returns the absolute value of **x**.   

`CEIL` (**x**: _numeric_) → _numeric_      
&emsp;Alias of `CEILING`(**x**).  

`CEILING` (**x**: _numeric_) → _numeric_   
&emsp;Returns **x** rounded up to the nearest integer.    

`FLOOR` (**x**: _numeric_) → _numeric_   
&emsp;Returns **x** rounded down to the nearest integer.   

`PI` () → _numeric_   
&emsp;Returns the constant Pi.  

`ROUND` (**x**: _numeric_) → _numeric_ ♫     
&emsp;Returns **x** rounded to the nearest integer. 

`ROUND` (**x**: _numeric_, **places**: _numeric_) → _numeric_ ♫     
&emsp;Returns **x** rounded to **places** decimal places.

`TRUNC` (**x**: _numeric_) → _numeric_     
&emsp;Alias of `TRUNCATE`(**x**).  

`TRUNCATE` (**x**: _numeric_) → _numeric_    
&emsp;Returns **x** rounded to integer by dropping digits after decimal point.    

## Text Functions

Functions for examining and manipulating string values. 

**str**: _varchar_`[`**index**: _numeric_`]` → _varchar_ ♫  
&emsp;Return the **index**th character from **str**. 

`GET` (**str**: _varchar_, **index**: _numeric_) → _varchar_ ♫   
&emsp;Alias of **str**`[`**index**`]`.   

`LEFT` (**str**: _varchar_, **n**: _numeric_) → _varchar_    
&emsp;Extract the left-most **n** characters of **str**.  

`LEN` (**str**: _varchar_) → _numeric_ ♫   
&emsp;Alias of `LENGTH`(**str**)

`LENGTH` (**str**: _varchar_) → _numeric_ ♫   
&emsp;Returns the length of **str** in characters.    

`LOWER` (**str**: _varchar_) → _varchar_   
&emsp;Converts **str** to lowercase.

`RIGHT` (**str**: _varchar_, **n**: _numeric_) → _varchar_    
&emsp;Extract the right-most **n** characters of **str**.   

`SEARCH` (**str**: _varchar_, **value**: _varchar_) → _boolean_ ♫    
&emsp;Return True if **str** contains **value**.   

`TRIM` (**str**: _varchar_) → _varchar_   
&emsp;Removes leading and trailing whitespace from **str**.  

`UPPER` (**str**: _varchar_) → _varchar_   
&emsp;Converts **str** to uppercase.  

## Date and Time Functions

For more details, see [Working with Timestamps](https://mabel-dev.github.io/opteryx/SQL%20Reference/Working%20with%20SQL/10%20Working%20with%20Timestamps/).

`current_date` → _timestamp_      
&emsp;Return the current date, in UTC. Note `current_date` does not require parenthesis.  

`current_time` → _timestamp_      
&emsp;Return the current date and time, in UTC. Note `current_time` does not require parenthesis.  

`DATE` (**ts**: _timestamp_) → _timestamp_      
&emsp;Remove any time information, leaving just the date part of **ts**.   

`DATE_FORMAT` (**ts**: _timestamp_, **format**: _varchar_) → _varchar_      
&emsp;Formats **ts** as a string using **format**.   

`DATE_PART`(**unit**: _varchar_, **ts**: _timestamp_) → _numeric_      
&emsp;Alias of `EXTRACT`(**unit** FROM **ts**).

`DATE_TRUNC` (**unit**: _varchar_, **ts**: _timestamp_) → _varchar_      
&emsp;Returns **ts** truncated to **unit**.  

`DATEDIFF` (**unit**: _varchar_, **start**: _timestamp_, **end**: _timestamp_) → _numeric_      
&emsp;Calculate the difference between the start and end timestamps in a given **unit**.  

`EXTRACT` (**unit** FROM _timestamp_) → _numeric_     
&emsp;Extract **unit** of a timestamp.

`NOW` () → _timestamp_   
&emsp;Alias for `current_time`.

`TIME` () → _timestamp_      
&emsp;Current Time (UTC).     

`TODAY` () → _timestamp_   
&emsp;Alias for `current_date`.

Recognized date parts and periods and support across various functions:

Part     | DATE_TRUNC | EXTRACT | DATEDIFF | Notes
-------- | :--------: | :-----: | :------: | ----
second   | ✓ | ✓ | ✓ |
minute   | ✓ | ✓ | ✓ |
hour     | ✓ | ✓ | ✓ |
day      | ✓ | ✓ | ✓ |
dow      | ✘ | ✓ | ✘ | day of week
week     | ✓ | ✓ | ✓ | iso week i.e. to monday
month    | ✓ | ✓ | ▲ | DATEFIFF unreliable calculating months
quarter  | ✓ | ✓ | ✓ |
doy      | ✘ | ✓ | ✘ | day of year
year     | ✓ | ✓ | ✓ |

The following convenience extraction functions also exist, however use of `EXTRACT` is recommended.

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

## Conversion

`CAST` (**any**: _any_ AS **type**) → _[type]_   
&emsp;Cast **any** to **type**.   

`NUMERIC` (_any_) → _numeric_      
&emsp;Convert **any** to a floating point number. 

`VARCHAR` (_any_) → _varchar_   
&emsp;Convert **any** to a string.

`STRING` (_any_) → _varchar_   
&emsp;Alias of `VARCHAR`(_any_)

`BOOLEAN` (_any_) → _boolean_        
&emsp;Convert input to a Boolean   

`TIMESTAMP` (_varchar_) → _timestamp_ ♫        
&emsp;Convert an [ISO 8601](https://www.iso.org/iso-8601-date-and-time-format.html) format string to a timestamp.  

`TIMESTAMP` (**seconds**: _numeric_) → _timestamp_ ♫     
&emsp;Return timestamp of **seconds** seconds since the Unix Epoch. 

## Struct Functions

For more details, see [Working with Structs](https://mabel-dev.github.io/opteryx/SQL%20Reference/Working%20with%20SQL/30%20Working%20with%20Structs/).

**object**: _struct_`[`**key**: _varchar_`]` → **value** ♫  
&emsp;Return the value for **key** from **object**. 

`GET` (**object**: _struct_, **key**: _varchar_) → **value** ♫   
&emsp;Alias of **object**`[`**key**`]`.  

`SEARCH` (**object**: _struct_, **value**: _varchar_) → **boolean** ♫  
&emsp;Return `true` if any of the values in **object** is **value**.

## System Functions

`VERSION` () → _varchar_        
&emsp;Return the version of Opteryx.

## Other Functions

`COALESCE` (**arg1**, **arg2**, ...) → _[input type]_   
&emsp;Return the first item from args which is not `null`.   

`GENERATE_SERIES` (**stop**: _numeric_) → _list_<_numeric_> ♫       
&emsp;Return a numeric list between 1 and **stop**, with a step of 1.    

`GENERATE_SERIES` (**start**: _numeric_, **stop**: _numeric_) → _list_<_numeric_> ♫       
&emsp;Return a numeric list between **start** and **stop**, with a step of 1.

`GENERATE_SERIES` (**start**: _numeric_, **stop**: _numeric_, **step**: _numeric_) → _list_<_numeric_> ♫       
&emsp;Return a numeric list between **start** and **stop**, with an increment of **step**.

`GENERATE_SERIES` (**start**: _timestamp_, **stop**: _timestamp_, _interval_) → _list_<_timestamp_> ♫       
&emsp;Return a timestamp list between **start** and **stop**, with a interval of **step**.    

`GENERATE_SERIES` (**cidr**: _varchar_) → _list_<_varchar_> ♫       
&emsp;Return a list of IP addresses from a given **cidr**.   

`HASH` (**any**) → _varchar_           
&emsp;Calculate the [CityHash](https://opensource.googleblog.com/2011/04/introducing-cityhash.html) (64 bit).

`MD5` (**any**) → _varchar_     
&emsp;Calculate the MD5 hash.

`RANDOM` () → _numeric_       
&emsp;Random number between 0.000 and 0.999.

`UNNEST` (**array**: _list_) → _relation_       
&emsp;Create a virtual relation with a row for each element in **array**.

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

