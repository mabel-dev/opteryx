# Expressions

An expression is a combination of values, operators and functions. Expressions are highly composable, and range from very simple to arbitrarily complex. They can be found in many different parts of SQL statements. In this section, we provide the different types of operators that can be used within expressions.

## Logical Operators

Logical Operators are used within Expressions to express how predicates combine.

The following logical operators are available: `NOT`, `AND`, `OR`, and `XOR`.

| a     | b     | a `AND` b | a `OR` b | a `XOR` b |
| :---: | :---: | :-------: | :------: | :-------: |
| True  | True  | True      | True     | False     |
| True  | False | False     | True     | True      |
| False | False | False     | False    | False     |
| Null  | True  | Null      | Null     | Null      |
| Null  | False | Null      | Null     | Null      |

The operators `AND`, `OR`, and `XOR` are commutative, that is, you can switch the left and right operand without affecting the result.

## Comparison Operators

Comparison Operators are used within Expressions to compare values, usually involving comparing a field within the datasets against a literal value - although comparisons can be used against two fields, or two literals.

Usually when one of the values involved in the comparison is `NULL`, the result is `NULL`.

Operator     | Description                   
:----------- | :-----------------------------
`=`          | equal to               
`<>`         | not equal to  
`<`          | less than                     
`>`          | greater than                
`<=`         | less than or equal to        
`>=`         | greater than or equal to                  
`IN`         | value in list              
`NOT IN`     | value not in list            
`LIKE`       | pattern match           
`NOT LIKE`   | inverse results of `LIKE`         
`ILIKE`      | case-insensitive pattern match 
`NOT ILIKE`  | inverse results of `ILIKE`     
`~`          | regular expression match (also `SIMILAR TO`)     
`!~`         | inverse results of `~` (also `NOT SIMILAR TO`)
`~*`         | case insensitive regular expression match
`!~*`        | inverse results of `~*`
`IS`         | special comparison for non-values `True`, `False` and `Null`

!!! Note  
    When handling `NULL` values, infix inversions (e.g. `x NOT LIKE y`) behave differently to prefix inversions (`NOT x LIKE y`).

## Other Comparisons

Predicate               | Description
----------------------- | ---------------------------------
`a BETWEEN x AND y`     | equivalent to `a >= x AND a <= y`
`a NOT BETWEEN x AND y` | equivalent to `a < x OR a > y`

!!! Warning  
    Using `BETWEEN` with other predicates, especially when used with an `AND` conjunction, can cause the query parser to fail. 

## Subqueries

The `IN` operator can reference a sub query, this sub query cannot include a temporal clause (`FOR`), but otherwise the full syntax for `SELECT` queries are supported.

For example, to find the planets without any satellites.

~~~sql
SELECT name
  FROM $planets
 WHERE id NOT IN (
     SELECT DISTINCT planetId
       FROM $satellites
    );
~~~
