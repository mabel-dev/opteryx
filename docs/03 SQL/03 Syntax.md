# SQL Syntax

## SELECT

## FROM

## WHERE

## FOR

`FOR DATES BETWEEN A AND B`

`FOR DATES AS OF timestamp`

`FOR TODAY`

`FOR YESTERDAY`

**limitations**

`FOR` clauses cannot contain comments or reference column values or aliases

## GROUP BY

**Limitations**

Can `GROUP BY` functions in the `SELECT` clause but not by aliases.

Can't `GROUP BY` the result of the `CAST` function, can can `GROUP BY` individual type cast functions like `NUMERIC`, `VARCHAR` and `BOOLEAN`.

## HAVING

## ORDER BY

## LIMIT

## OFFSET

## VALUES