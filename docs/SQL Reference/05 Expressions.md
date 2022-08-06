# Expressions

## Logical

The following logical operators are available: `NOT`, `AND`, `OR`, and `XOR`.

| a     | b     | a `AND` b | a `OR` b | a `XOR` b |
| :---: | :---: | :-------: | :------: | :-------: |
| TRUE  | TRUE  | TRUE      | TRUE     | FALSE     |
| TRUE  | FALSE | FALSE     | TRUE     | TRUE      |
| FALSE | FALSE | FALSE     | FALSE    | FALSE     |

The operators `AND`, `OR`, and `XOR` are commutative, that is, you can switch the left and right operand without affecting the result.

## Comparison Operators

Operator     | Description                   
:----------- | :-----------------------------
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
`!~`         | inverse of `~`

!!! note
    When handling `null` and `none` values, infix inversions (e.g. `x NOT LIKE y`) behave differently to prefix inversions (`NOT x LIKE y`).

## Other Comparisons

Predicate               | Description
----------------------- | ---------------------------------
`a BETWEEN x AND y`     | equivalent to `a >= x AND a <= y`
`a NOT BETWEEN x AND y` | equivalent to `a < x OR a > y`

Using `BETWEEN` with other predicates, especially when used with an `AND` conjunction, can cause the query parser to fail. 

## Sub Queries

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
