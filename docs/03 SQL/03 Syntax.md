# SQL Syntax

## FROM

## WHERE

`$DATE` is a special term, allowing you to specify partitions in SQL, can be
used in WHERE clauses like any other field. If used, it will replace any dates
provided as parameters.

`WHERE $DATE BETWEEN <start_date> AND <end_date>`
`WHERE $DATE > '2021-01-02'`
`WHERE $DATE = TODAY()`


## GROUP BY

## HAVING

## ORDER BY

## LIMIT

## CREATE

## WITH