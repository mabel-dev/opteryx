# SQL Expressions

## Logical

* NOT <- to be implemented

The following logical operators are available: `AND` and `OR`.

a     | b     | a AND b | a OR b
----- | ----- | ------- | -------
TRUE  | TRUE  | TRUE    | TRUE
TRUE  | FALSE | FALSE   | TRUE
FALSE | FALSE | FALSE   | FALSE

The operators `AND` and `OR` are commutative, that is, you can switch the left and right operand without affecting the result.

## Comparison Operators

Operator     | Description                    | Example | Result
------------ | ------------------------------ | ------- | ------
`<`          | less than                      | 2 < 3   | TRUE
`>`          | greater than                   | 2 > 3   | FALSE
`<=`         | less than or equal to          | 2 <= 3  | TRUE
`>=`         | greater than or equal to       | 4 >= 8  | TRUE
`=`          | equal to                       | 3 = 4   | FALSE
`<>`         | not equal to                   | 2 <> 2  | FALSE
`IN`         | value in list                  |         |
`NOT IN`     | value not in list              |         |
`LIKE`       | pattern match                  |         |
`NOT LIKE`   | inverse of `LIKE`              |         |
`ILIKE`      | case-insensitive pattern match |         |
`NOT ILIKE`  | inverse of `ILIKE`             |         |
`~`          | regular expression match       |         | 

## Other Comparisons

Predicate               | Description
----------------------- | ---------------------------------
`a BETWEEN x AND y`     | equivalent to `a >= x AND a <= y`
`a NOT BETWEEN x AND y` | equivalent to `a < x OR a > y`
