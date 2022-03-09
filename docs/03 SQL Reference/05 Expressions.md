# SQL Expressions

## Logical

The following logical operators are available: `AND` and `OR`.

a     | b     | a AND b | a OR b
----- | ----- | ------- | -------
TRUE  | TRUE  | TRUE    | TRUE
TRUE  | FALSE | FALSE   | TRUE
FALSE | FALSE | FALSE   | FALSE

The operators `AND` and `OR` are commutative, that is, you can switch the left and right operand without affecting the result.

## Comparison Operators

Operator     | Description                   
------------ | ------------------------------
`<`          | less than                     
`>`          | greater than                
`<=`         | less than or equal to        
`>=`         | greater than or equal to   
`=`          | equal to               
`<>`         | not equal to                 
`IN`         | value in list              
`NOT IN`     | value not in list            
`LIKE`       | pattern match           
`NOT LIKE`   | inverse of `LIKE`         
`ILIKE`      | case-insensitive pattern match 
`NOT ILIKE`  | inverse of `ILIKE`     
`~`          | regular expression match      

## Other Comparisons

Predicate               | Description
----------------------- | ---------------------------------
`a BETWEEN x AND y`     | equivalent to `a >= x AND a <= y`
`a NOT BETWEEN x AND y` | equivalent to `a < x OR a > y`

Using `BETWEEN` with other predicates, especially when used with an `AND` conjunction, can cause the query parser to fail. 

## Subqueries

The `IN` operator can reference a subquery, this subquery cannot include a temporal clause (`FOR`), but otherwise the full syntax for `SELECT` queries are supported.

For example to find the planets without any satellites.
~~~sql
SELECT name
  FROM $planets
 WHERE id NOT IN (
     SELECT DISTINCT planetId
       FROM $satellites
    )
~~~
