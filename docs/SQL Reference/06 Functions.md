# Functions

Definitions noted with a 🔻 accept different input arguments.

## List Functions

For more details, see [Working with Lists](https://mabel-dev.github.io/opteryx/SQL%20Reference/Working%20with%20SQL/20%20Working%20with%20Lists/).

!!! function "**array**: _list_`[`**index**: _numeric_`]` → **value** 🔻"  
    Return the **index**th element from **array**. 

!!! function "`GET` (**array**: _list_, **index**: _numeric_) → **value** 🔻"   
    Alias of **array**`[`**index**`]`.  

!!! function "`LEN` (**array**: _list_) → _numeric_ 🔻"   
    Alias of `LENGTH`(**array**).

!!! function "`LENGTH` (**array**: _list_) → _numeric_ 🔻"   
    Returns the number of elements in **array**.

!!! function "`LIST_CONTAINS` (**array**: _list_, **value**) → _boolean_"
    Return `true` if **array** contains **value**. See also `SEARCH`(**array**, **value**).

!!! function "`LIST_CONTAINS_ANY` (**array**: _list_, **values**: _list_) → _boolean_"
    Return `true` if **array** contains any elements in **values**.

!!! function "`LIST_CONTAINS_ALL` (**array**: _list_, **values**: _list_) → _boolean_"
    Return `true` if **array** contains all of elements in **values**.

!!! function "`SEARCH` (**array**: _list_, **value**) → _boolean_ 🔻"
    Return `true` if **array** contains **value**. 

## Numeric Functions

!!! function "`ABS` (**x**: _numeric_) → _numeric_"
    Alias of `ABSOLUTE`(**x**).  

!!! function "`ABSOLUTE` (**x**: _numeric_) → _numeric_"
    Returns the absolute value of **x**.   

!!! function "`CEIL` (**x**: _numeric_) → _numeric_"
    Alias of `CEILING`(**x**).  

!!! function "`CEILING` (**x**: _numeric_) → _numeric_"
    Returns **x** rounded up to the nearest integer.    

!!! function "`FLOOR` (**x**: _numeric_) → _numeric_"
    Returns **x** rounded down to the nearest integer.   

!!! function "`PI` () → _numeric_"
    Returns the constant Pi.  

!!! function "`ROUND` (**x**: _numeric_) → _numeric_ 🔻"
    Returns **x** rounded to the nearest integer. 

!!! function "`ROUND` (**x**: _numeric_, **places**: _numeric_) → _numeric_ 🔻"
    Returns **x** rounded to **places** decimal places.

!!! function "`TRUNC` (**x**: _numeric_) → _numeric_"
    Alias of `TRUNCATE`(**x**).  

!!! function "`TRUNCATE` (**x**: _numeric_) → _numeric_"
    Returns **x** rounded to integer by dropping digits after decimal point.    

## Text Functions

Functions for examining and manipulating string values. 

!!! function "**str**: _varchar_`[`**index**: _numeric_`]` → _varchar_ 🔻"
    Subscript operator, return the **index**th character from **str**. 

!!! function "`GET` (**str**: _varchar_, **index**: _numeric_) → _varchar_ 🔻"
    Alias of **str**`[`**index**`]`.   

!!! function "`LEFT` (**str**: _varchar_, **n**: _numeric_) → _varchar_"
    Extract the left-most **n** characters of **str**.  

!!! function "`LEN` (**str**: _varchar_) → _numeric_ 🔻" 
    Alias of `LENGTH`(**str**)

!!! function "`LENGTH` (**str**: _varchar_) → _numeric_ 🔻"
    Returns the length of **str** in characters.    

!!! function "`LOWER` (**str**: _varchar_) → _varchar_"
    Converts **str** to lowercase.

!!! function "`RIGHT` (**str**: _varchar_, **n**: _numeric_) → _varchar_"
    Extract the right-most **n** characters of **str**.   

!!! function "`SEARCH` (**str**: _varchar_, **value**: _varchar_) → _boolean_ 🔻"
    Return True if **str** contains **value**.   

!!! function "`TRIM` (**str**: _varchar_) → _varchar_"
    Removes leading and trailing whitespace from **str**.  

!!! function "`UPPER` (**str**: _varchar_) → _varchar_"
    Converts **str** to uppercase.  

## Date and Time Functions

For more details, see [Working with Timestamps](https://mabel-dev.github.io/opteryx/SQL%20Reference/Working%20with%20SQL/10%20Working%20with%20Timestamps/).

!!! function "`current_date` → _timestamp_"
    Return the current date, in UTC. Note `current_date` does not require parenthesis.  

!!! function "`current_time` → _timestamp_"
    Return the current date and time, in UTC. Note `current_time` does not require parenthesis.  

!!! function "`DATE` (**ts**: _timestamp_) → _timestamp_"
    Remove any time information, leaving just the date part of **ts**.   

!!! function "`DATE_FORMAT` (**ts**: _timestamp_, **format**: _varchar_) → _varchar_"
    Formats **ts** as a string using **format**.   

!!! function "`DATEPART`(**unit**: _varchar_, **ts**: _timestamp_) → _numeric_"
    Alias of `EXTRACT`(**unit** FROM **ts**).

!!! function "`DATE_TRUNC` (**unit**: _varchar_, **ts**: _timestamp_) → _varchar_"
    Returns **ts** truncated to **unit**.  

!!! function "`DATEDIFF` (**unit**: _varchar_, **start**: _timestamp_, **end**: _timestamp_) → _numeric_"
    Calculate the difference between the start and end timestamps in a given **unit**.  

!!! function "`DAY` (_timestamp_) → _numeric_"
    Extract day number from a timestamp. See `EXTRACT`.

!!! function "`EXTRACT` (**unit** FROM _timestamp_) → _numeric_"     
    Extract **unit** of a timestamp.   
    Also implemented as individual extraction functions.

!!! function "`NOW` () → _timestamp_"
    Alias for `current_time`.

!!! function "`TIME` () → _timestamp_"
    Current Time (UTC).     

!!! function "`TIME_BUCKET` (_timestamp_, **multiple**: _numeric_, **unit**: _varchar_) → _timestamp_"
    Floor timestamps into fixed time interval buckets. **unit** is optional and will be `day` if not provided.

!!! function "`TODAY` () → _timestamp_"
    Alias for `current_date`.

!!! function "`HOUR` (**ts**: _timestamp_) → _numeric_"
    Returns the hour of the day from **ts**. The value ranges from `0` to `23`.   
    Alias for `EXTRACT`(hour FROM **ts**).

!!! function "`MINUTE` (**ts**: _timestamp_) → _numeric_"
    Returns the minute of the hour from **ts**. The value ranges from `0` to `59`.  
    Alias for `EXTRACT`(minute FROM **ts**)

!!! function "`MONTH` (**ts**: _timestamp_) → _numeric_"
    Returns the month of the year from **ts**. The value ranges from `1` to `12`.  
    Alias for `EXTRACT`(month FROM **ts**)

!!! function "`QUARTER` (**ts**: _timestamp_) → _numeric_"
    Returns the quarter of the year from **ts**. The value ranges from `1` to `4`.  
    Alias for `EXTRACT`(quarter FROM **ts**)

!!! function "`SECOND` (**ts**: _timestamp_) → _numeric_"
    Returns the second of the minute from **ts**. The value ranges from `0` to `59`.  
    Alias for `EXTRACT`(second FROM **ts**)

!!! function "`WEEK` (**ts**: _timestamp_) → _numeric_"
    Returns the week of the year from **ts**. The value ranges from `1` to `53`.  
    Alias for `EXTRACT`(week FROM **ts**)

!!! function "`YEAR` (**ts**: _timestamp_) → _numeric_"
    Returns the year from **ts**.  
    Alias for `EXTRACT`(year FROM **ts**)

## Conversion Functions

!!! function "`BOOLEAN` (**any**: _any_) → _boolean_"
    Cast **any** to a `boolean`, raises an error if cast is not possible.   
    Alias for `CAST`(**any** AS BOOLEAN).   

!!! function "`CAST` (**any**: _any_ AS **type**) → _[type]_"
    Cast **any** to **type**, raises an error if cast is not possible.   
    Also implemented as individual cast functions.

!!! function "`NUMERIC` (**any**: _any_) → _numeric_"
    Cast **any** to a floating point number, raises an error if cast is not possible.   
    Alias for `CAST`(**any** AS NUMERIC).   

!!! function "`STRING` (**any**: _any_) → _varchar_"   
    Alias of `VARCHAR`(**any**) and `CAST`(**any** AS VARCHAR)

!!! function "`TIMESTAMP` (**iso8601**: _varchar_) → _timestamp_ 🔻"
    Cast an [ISO 8601](https://www.iso.org/iso-8601-date-and-time-format.html) format string to a timestamp, raises an error if cast is not possible.  
    Alias for `CAST`(**iso8601** AS TIMESTAMP).   

!!! function "`TIMESTAMP` (**seconds**: _numeric_) → _timestamp_ 🔻"
    Return timestamp of **seconds** seconds since the Unix Epoch. 

!!! function "`TRY_CAST` (**any**: _any_ AS **type**) → _[type]_"
    Cast **any** to **type**, if cast is not possible, returns `NULL`.   

!!! function "`VARCHAR` (_any_) → _varchar_"
    Cast **any** to a string, raises an error if cast is not possible.  
    Alias for `CAST`(**any** AS VARCHAR).

## Struct Functions

For more details, see [Working with Structs](https://mabel-dev.github.io/opteryx/SQL%20Reference/Working%20with%20SQL/30%20Working%20with%20Structs/).

!!! function "**object**: _struct_`[`**key**: _varchar_`]` → **value** 🔻"
    Subscript operator, return the value for **key** from **object**. 

!!! function "`GET` (**object**: _struct_, **key**: _varchar_) → **value** 🔻"
    Alias of **object**`[`**key**`]`.  

!!! function "`SEARCH` (**object**: _struct_, **value**: _varchar_) → **boolean** 🔻"
    Return `TRUE` if any of the values in **object** is **value**. Note `SEARCH` does not match struct keys.

## System Functions

!!! function "`VERSION` () → _varchar_"
    Return the version of Opteryx.

## Infix Function

These are functions that are called similar to comparison operators:

!!! function "_numeric_ `+` _numeric_ → _numeric_"
    Numeric addition

!!! function "_numeric_ `-` _numeric_ → _numeric_"
    Numeric subtraction

!!! function "_numeric_ `*` _numeric_ → _numeric_"
    Numeric multiplication

!!! function "_numeric_ `/` _numeric_ → _numeric_"
    Numeric division

!!! function "_numeric_ `%` _numeric_ → _numeric_"
    Numeric modulo (remainder)

!!! function "_varchar_ `||` _varchar_ → _varchar_"
    String concatenation  

## Other Functions

!!! function "`COALESCE` (**arg1**, **arg2**, ...) → _[input type]_"
    Return the first item from args which is not `NULL`.   

!!! function "`GENERATE_SERIES` (**stop**: _numeric_) → _list_<_numeric_> 🔻"
    Return a numeric list between 1 and **stop**, with a step of 1.  

!!! function "`GENERATE_SERIES` (**start**: _numeric_, **stop**: _numeric_) → _list_<_numeric_> 🔻" 
    Return a numeric list between **start** and **stop**, with a step of 1.

!!! function "`GENERATE_SERIES` (**start**: _numeric_, **stop**: _numeric_, **step**: _numeric_) → _list_<_numeric_> 🔻"
    Return a numeric list between **start** and **stop**, with an increment of **step**.

!!! function "`GENERATE_SERIES` (**start**: _timestamp_, **stop**: _timestamp_, _interval_) → _list_<_timestamp_> 🔻"
    Return a timestamp list between **start** and **stop**, with a interval of **step**.    

!!! function "`GENERATE_SERIES` (**cidr**: _varchar_) → _list_<_varchar_> 🔻"
    Return a list of IP addresses from a given **cidr**.   

!!! function "`HASH` (**any**) → _varchar_"
    Calculate the [CityHash](https://opensource.googleblog.com/2011/04/introducing-cityhash.html) (64 bit).

!!! function "`MD5` (**any**) → _varchar_"
    Calculate the MD5 hash.

!!! function "`RANDOM` () → _numeric_"
    Random number between 0.0000 and 0.9999.

!!! function "`UNNEST` (**array**: _list_) → _relation_"
    Create a virtual relation with a row for each element in **array**.

