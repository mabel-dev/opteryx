# SQL Data Types

## Types

Name      | Description
--------- | ------------------------------------------
BOOLEAN   | Logical boolean (True/False).
NUMERIC   | All numeric types
LIST      | An ordered sequence of data values.
VARCHAR   | Variable-length character string.
STRUCT    | A dictionary of multiple named values, where each key is a string, but the value can be a different type for each key.
TIMESTAMP | Combination of date and time.
OTHER     | None of the above or multiple types in the same column. 

## BOOLEAN

Columns of type BOOLEAN cannot be indexed. 

## NUMERIC

Type Hint in Queries NUMERIC('value')

## LIST

Columns of type LIST cannot be indexed.

Formatted as (value1, value2, ...)

## VARCHAR
## STRUCT

Columns of type STRUCT cannot be indexed. 

## TIMESTAMP

Mabel will implicitly interpret strings formatted as

'YYYY-MM-DD' or 'YYYY-MM-DD HH:MM' as TIMESTAMP

Type Hint in Queries TIMESTAMP('value')

## OTHER

DataSets with columns of type OTHER cannot have DISTINCT functions applied.

Columns of type OTHER cannot be indexed. 
