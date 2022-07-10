# Functions

Definitions noted with a ♫ accept different input arguments.

!!! note
    Functions presently cannot be used with the outputs of function calls, for example `DATEDIFF('year', birth_date, TODAY())` will return an error.

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

`DATEPART`(**unit**: _varchar_, **ts**: _timestamp_) → _numeric_      
&emsp;Alias of `EXTRACT`(**unit** FROM **ts**).

`DATE_TRUNC` (**unit**: _varchar_, **ts**: _timestamp_) → _varchar_      
&emsp;Returns **ts** truncated to **unit**.  

`DATEDIFF` (**unit**: _varchar_, **start**: _timestamp_, **end**: _timestamp_) → _numeric_      
&emsp;Calculate the difference between the start and end timestamps in a given **unit**.  

`DAY` (_timestamp_) → _numeric_  
&emsp;Extract day number from a timestamp. See `EXTRACT`.

`EXTRACT` (**unit** FROM _timestamp_) → _numeric_     
&emsp;Extract **unit** of a timestamp.
&emsp;Also implemented as individual extraction functions.

`NOW` () → _timestamp_   
&emsp;Alias for `current_time`.

`TIME` () → _timestamp_      
&emsp;Current Time (UTC).     

`TODAY` () → _timestamp_   
&emsp;Alias for `current_date`.

`HOUR` (**ts**: _timestamp_) → _numeric_  
&emsp;Returns the hour of the day from **ts**. The value ranges from `0` to `23`.  
&emsp;Alias for `EXTRACT`(hour FROM **ts**).

`MINUTE` (**ts**: _timestamp_) → _numeric_  
&emsp;Returns the minute of the hour from **ts**. The value ranges from `0` to `59`.  
&emsp;Alias for `EXTRACT`(minute FROM **ts**)

`MONTH` (**ts**: _timestamp_) → _numeric_  
&emsp;Returns the month of the year from **ts**. The value ranges from `1` to `12`.  
&emsp;Alias for `EXTRACT`(month FROM **ts**)

`QUARTER` (**ts**: _timestamp_) → _numeric_  
&emsp;Returns the quarter of the year from **ts**. The value ranges from `1` to `4`.  
&emsp;Alias for `EXTRACT`(quarter FROM **ts**)

`SECOND` (**ts**: _timestamp_) → _numeric_  
&emsp;Returns the second of the minute from **ts**. The value ranges from `0` to `59`.  
&emsp;Alias for `EXTRACT`(second FROM **ts**)

`WEEK` (**ts**: _timestamp_) → _numeric_  
&emsp;Returns the week of the year from **ts**. The value ranges from `1` to `53`.  
&emsp;Alias for `EXTRACT`(week FROM **ts**)

`YEAR` (**ts**: _timestamp_) → _numeric_  
&emsp;Returns the year from **ts**.  
&emsp;Alias for `EXTRACT`(year FROM **ts**)

## Conversion Functions

`BOOLEAN` (**any**: _any_) → _boolean_        
&emsp;Cast **any** to a `boolean`.   
&emsp;Alias for `CAST`(**any** AS BOOLEAN).   

`CAST` (**any**: _any_ AS **type**) → _[type]_   
&emsp;Cast **any** to **type**.   
&emsp;Also implemented as individual cast functions.

`NUMERIC` (**any**: _any_) → _numeric_      
&emsp;Cast **any** to a floating point number.   
&emsp;Alias for `CAST`(**any** AS NUMERIC).   

`STRING` (**any**: _any_) → _varchar_   
&emsp;Alias of `VARCHAR`(**any**) and `CAST`(**any** AS VARCHAR)

`TIMESTAMP` (**iso8601**: _varchar_) → _timestamp_ ♫        
&emsp;Cast an [ISO 8601](https://www.iso.org/iso-8601-date-and-time-format.html) format string to a timestamp.  
&emsp;Alias for `CAST`(**iso8601** AS TIMESTAMP).   

`TIMESTAMP` (**seconds**: _numeric_) → _timestamp_ ♫     
&emsp;Return timestamp of **seconds** seconds since the Unix Epoch. 

`TRY_CAST` (**any**: _any_ AS **type**) → _[type]_   
&emsp;Cast **any** to **type**, failures return `null`.   

`VARCHAR` (_any_) → _varchar_   
&emsp;Cast **any** to a string.  
&emsp;Alias for `CAST`(**any** AS VARCHAR).

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
