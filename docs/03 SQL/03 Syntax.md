# SQL Syntax

## FROM

## WHERE

`$PARTITION` is a special term, allowing you to specify partitions in SQL, can be
used in WHERE clauses like any other field. If used, it will replace any dates
provided as parameters.

`WHERE $PARTITION BETWEEN <start_date> AND <end_date>`
`WHERE $PARTITION > '2021-01-02'`


## GROUP BY

## HAVING

## ORDER BY

## LIMIT

## CREATE

## WITH