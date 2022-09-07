# Functions

Definitions noted with a 🔻 accept different input arguments.

## Conversion Functions

!!! function "`BOOLEAN` (**any**: _any_) → _boolean_"  
    Cast **any** to a `BOOLEAN`, raises an error if cast is not possible.   
    Alias for `CAST`(**any** AS BOOLEAN).   

!!! function "`CAST` (**any**: _any_ AS **type**) → _[type]_"  
    Cast **any** to **type**, raises an error if cast is not possible.   
    Also implemented as individual cast functions.

!!! function "`INT` (**num**: _numeric_) → _numeric_"  
    Alias for `INTEGER`.

!!! function "`INTEGER` (**num**: _numeric_) → _numeric_"  
    Convert **num** to an integer.   
    `INTEGER` is a psuedo-type, `CAST` is not supported and values may be coerced to `NUMERIC`.

!!! function "`FLOAT` (**num**: _numeric_) → _numeric_"  
    Convert **num** to a floating point number.   
    `FLOAT` is a psuedo-type, `CAST` is not supported and values may be coerced to `NUMERIC`..

!!! function "`NUMERIC` (**any**: _any_) → _numeric_"  
    Cast **any** to a floating point number, raises an error if cast is not possible.   
    Alias for `CAST`(**any** AS NUMERIC).   

!!! function "`SAFE_CAST` (**any**: _any_ AS **type**) → _[type]_"    
    Alias for `TRY_CAST`(**any** AS **type**).  

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

## Infix Function

These are functions that are called similar to comparison operators:

!!! function "_numeric_ `+` _numeric_ → _numeric_"  
    Numeric addition

!!! function "_timestamp_ `+` _interval_ → _timestamp_"  
    Timestamp and Interval addition

!!! function "_numeric_ `-` _numeric_ → _numeric_"  
    Numeric subtraction

!!! function "_timestamp_ `-` _interval_ → _timestamp_"  
    Timestamp and Interval subtraction

!!! function "_timestamp_ `-` _timestamp_ → _interval_"  
    Timestamp subtraction

!!! function "_numeric_ `*` _numeric_ → _numeric_"  
    Numeric multiplication

!!! function "_numeric_ `/` _numeric_ → _numeric_"  
    Numeric division

!!! function "_numeric_ `%` _numeric_ → _numeric_"  
    Numeric modulo (remainder)

!!! function "_varchar_ `||` _varchar_ → _varchar_"  
    String concatenation  

## List Functions

For more details, see [Working with Lists](https://mabel-dev.github.io/opteryx/SQL%20Reference/Working%20with%20SQL/20%20Working%20with%20Lists/).

!!! function "**array**: _list_`[`**index**: _numeric_`]` → **value** 🔻"  
    Return the **index**th element from **array**. 

!!! function "`GET` (**array**: _list_, **index**: _numeric_) → **value** 🔻"   
    Alias of **array**`[`**index**`]`.  

!!! function "`GREATEST` (**array**: _list_) → **value** 🔻"   
    Return the greatest value in **array**.  
    Related: `LEAST`.

!!! function "`LEAST` (**array**: _list_) → **value** 🔻"   
    Return the smallest value in **array**.  
    Related: `GREATEST`.

!!! function "`LEN` (**array**: _list_) → _numeric_ 🔻"   
    Alias of `LENGTH`(**array**).

!!! function "`LENGTH` (**array**: _list_) → _numeric_ 🔻"   
    Returns the number of elements in **array**.

!!! function "`LIST_CONTAINS` (**array**: _list_, **value**) → _boolean_"  
    Return `true` if **array** contains **value**.  
    See also `SEARCH`(**array**, **value**).  

!!! function "`LIST_CONTAINS_ANY` (**array**: _list_, **values**: _list_) → _boolean_"    
    Return `true` if **array** contains any elements in **values**.

!!! function "`LIST_CONTAINS_ALL` (**array**: _list_, **values**: _list_) → _boolean_"   
    Return `true` if **array** contains all of elements in **values**.

!!! function "`SEARCH` (**array**: _list_, **value**) → _boolean_ 🔻"  
    Return `true` if **array** contains **value**. 

!!! function "`SORT` (**array**: _list_) → _list_"  
    Return **array** in ascending order. 

## Numeric Functions

!!! function "`ABS` (**x**: _numeric_) → _numeric_"  
    Alias of `ABSOLUTE`.  

!!! function "`ABSOLUTE` (**x**: _numeric_) → _numeric_"  
    Returns the absolute value of **x**.   

!!! function "`CEIL` (**x**: _numeric_) → _numeric_"  
    Alias of `CEILING`.  

!!! function "`CEILING` (**x**: _numeric_) → _numeric_"  
    Returns **x** rounded up to the nearest integer.   
    Related: `FLOOR` 

!!! function "`E` () → _numeric_"  
    Returns the constant _e_, also known as _Euler's number_.  
    Related: `LN`.

!!! function "`FLOOR` (**x**: _numeric_) → _numeric_"  
    Returns **x** rounded down to the nearest integer.   

!!! function "`PHI` () → _numeric_"  
    Returns the constant φ (_phi_), also known as _the golden ratio_.  

!!! function "`PI` () → _numeric_"  
    Returns the constant π (_pi_).  

!!! function "`POWER` (**base**: _numeric_, **exponent**: _numeric**) → _numeric_"   
    Returns **base** to the power of **exponent**.  

!!! function "`LN` (**x**: _numeric_) → _numeric_"   
    Returns the natural logarithm of **x**.  
    Related: `E`, `LOG`, `LOG10`, `LOG2`.

!!! function "`LOG` (**x**: _numeric_, **base**: _numeric_) → _numeric_"   
    Returns the logarithm of **x** for base **base**.   
    Related: `LN`, `LOG10`, `LOG2`.

!!! function "`LOG10` (**x**: _numeric_) → _numeric_"   
    Returns the logarithm for base 10 of **x**.  
    Related: `LN`, `LOG`, `LOG2`.

!!! function "`LOG2` (**x**: _numeric_) → _numeric_"   
    Returns the logarithm for base 2 of **x**.  
    Related: `LN`, `LOG`, `LOG10`.

!!! function "`ROUND` (**x**: _numeric_) → _numeric_ 🔻"  
    Returns **x** rounded to the nearest integer. 

!!! function "`ROUND` (**x**: _numeric_, **places**: _numeric_) → _numeric_ 🔻"  
    Returns **x** rounded to **places** decimal places.

!!! function "`SIGN` (**x**: _numeric_) → _numeric_"   
    Returns the signum function of **x**; 0 if **x** is 0, -1 if **x** is less than 0 and 1 if **x** is greater than 0.

!!! function "`SIGNUM` (**x**: _numeric_) → _numeric_"   
    Alias for `SIGN`.

!!! function "`SQRT` (**x**: _numeric_) → _numeric_"   
    Returns the square root of **x**.

!!! function "`TRUNC` (**x**: _numeric_) → _numeric_"  
    Alias of `TRUNCATE`.  

!!! function "`TRUNCATE` (**x**: _numeric_) → _numeric_"  
    Returns **x** rounded to integer by dropping digits after decimal point.    

## String Functions

Functions for examining and manipulating string values. 

!!! function "**str**: _varchar_`[`**index**: _numeric_`]` → _varchar_ 🔻"  
    Subscript operator, return the **index**th character from **str**. 

!!! function "`CONCAT` (**list**: _array_<_varchar_>) → _varchar_"   
    Returns the result of concatenating, or joining, of two or more string values in an end-to-end manner.  
    Related: `CONCAT_WS`.

!!! function "`CONCAT_WS` (**separator**: _varchar_, **list**: _array_<_varchar_>) → _varchar_"   
    Returns the result of concatenating, or joining, of two or more string values with a **separator** used to delimit individual values.  
    Related: `CONCAT`.

!!! function "`ENDS_WITH` (**str**: _varchar_, **value**: _varchar_) → _boolean_"  
    Return True if **str** ends with **value**.  
    Related: `STARTS_WITH`.

!!! function "`GET` (**str**: _varchar_, **index**: _numeric_) → _varchar_ 🔻"  
    Alias of **str**`[`**index**`]`.   

!!! function "`LEFT` (**str**: _varchar_, **n**: _numeric_) → _varchar_"  
    Extract the left-most **n** characters of **str**.  
    Related: `RIGHT`

!!! function "`LEN` (**str**: _varchar_) → _numeric_ 🔻"   
    Alias of `LENGTH`

!!! function "`LENGTH` (**str**: _varchar_) → _numeric_ 🔻"  
    Returns the length of **str** in characters.    

!!! function "`LOWER` (**str**: _varchar_) → _varchar_"  
    Converts **str** to lowercase.   
    Related: `UPPER`, `TITLE`.

!!! function "`REVERSE` (**str**: _varchar_) → _varchar_"  
    Returns **str** with the characters in reverse order.

!!! function "`RIGHT` (**str**: _varchar_, **n**: _numeric_) → _varchar_"  
    Extract the right-most **n** characters of **str**.   
    Related: `LEFT`.

!!! function "`SOUNDEX` (**str**: _varchar_) → _varchar_"  
    Returns a character string containing the phonetic representation of char. See [Soundex 🡕](https://en.wikipedia.org/wiki/Soundex).   

!!! function "`SEARCH` (**str**: _varchar_, **value**: _varchar_) → _boolean_ 🔻"  
    Return True if **str** contains **value**.  

!!! function "`STARTS_WITH` (**str**: _varchar_, **value**: _varchar_) → _boolean_"  
    Return True if **str** starts with **value**.  
    Related: `ENDS_WITH`

!!! function "`TITLE` (**str**: _varchar_) → _varchar_"  
    Returns **str** with the first letter of each work in upper case.   
    Related: `LOWER`, `UPPER`.

!!! function "`TRIM` (**str**: _varchar_) → _varchar_"  
    Removes leading and trailing whitespace from **str**.  

!!! function "`UPPER` (**str**: _varchar_) → _varchar_"  
    Converts **str** to uppercase.   
    Related: `LOWER`, `TITLE`.

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

## Other Functions

!!! function "`BASE64_DECODE` (**any**) → _varchar_"  
    Decode a value which has been encoded using BASE64 encoding.  
    Related: `BASE64_ENCODE`.

!!! function "`BASE64_ENCODE` (**any**) → _varchar_"  
    Encode value with BASE64 encoding.  
    Related: `BASE64_DECODE`.

!!! function "`BASE85_DECODE` (**any**) → _varchar_"  
    Decode a value which has been encoded using BASE85 encoding.  
    Related: `BASE85_ENCODE`.

!!! function "`BASE85_ENCODE` (**any**) → _varchar_"  
    Encode value with BASE85 encoding.  
    Related: `BASE85_DECODE`.

!!! function "`COALESCE` (**arg1**, **arg2**, ...) → _[input type]_"  
    Return the first item from args which is not `NULL`.    
    Related: `IFNULL`.

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

!!! function "`IFNULL` (**check_expression**: _any_, **replacement_value**: _any_) → _[input type]_"  
    Returns **check_expression** if not `NULL`, otherwise returns **replacement_value**.
    Related: `COALESCE`.

!!! function "`HASH` (**any**) → _varchar_"  
    Calculate the [CityHash](https://opensource.googleblog.com/2011/04/introducing-cityhash.html) (64 bit).

!!! function "`HEX_DECODE` (**any**) → _varchar_"  
    Decode a value which has been encoded using HEX (BASE16) encoding.  
    Related: `HEX_ENCODE`.

!!! function "`HEX_ENCODE` (**any**) → _varchar_"  
    Encode value with HEX (BASE16) encoding.  
    Related: `HEX_DECODE`.

!!! function "`IIF` (**condition**, **true_value**, **false_value***) → _[input type]_"  
    Return the **true_value** if the condition evaluates to True, otherwise return the **false_value**.

!!! function "`NORMAL` () → _numeric_"  
    Random number from a normal (Gaussian) distribution; distribution is centred at 0.0 and have a standard deviation of 1.0.

!!! function "`MD5` (**any**) → _varchar_"  
    Calculate the MD5 hash.

!!! function "`RAND` () → _numeric_"  
    Returns a random number between 0 and 1.

!!! function "`RANDOM` () → _numeric_"  
    Alias of `RAND`().

!!! function "`RANDOM_STRING` (**length**: _numeric_) → _varchar_"  
    Returns a random string of lowercase alphabetic characters with a length of **length**.

!!! function "`SHA1` (**any**) → _varchar_"  
    Calculate the SHA1 hash.  
    Related: `SHA224`, `SHA256`, `SHA384`, `SHA512`.

!!! function "`SHA224` (**any**) → _varchar_"  
    Calculate the SHA224 hash.  
    Related: `SHA1`, `SHA256`, `SHA384`, `SHA512`.

!!! function "`SHA256` (**any**) → _varchar_"  
    Calculate the SHA256 hash.  
    Related: `SHA1`, `SHA224`, `SHA384`, `SHA512`.

!!! function "`SHA384` (**any**) → _varchar_"  
    Calculate the SHA384 hash.  
    Related: `SHA1`, `SHA224`, `SHA256`, `SHA512`.

!!! function "`SHA512` (**any**) → _varchar_"  
    Calculate the SHA512 hash.  
    Related: `SHA1`, `SHA224`, `SHA256`, `SHA384`.

!!! function "`UNNEST` (**array**: _list_) → _relation_"  
    Create a virtual relation with a row for each element in **array**.

