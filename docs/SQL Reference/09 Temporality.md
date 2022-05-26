# Temporality

Temporality, or the ability to view things as they were at a different point in time, can be a difficult concept to understand and use.

Partition schemes that supports temporal queries allow you to view data from a different date by using a `FOR` clause in the SQL statement. `FOR` clauses state the date, or date range, a query should retrieve results for.

!!! note  
    If no temporal clause is provided and the schema supports it, `FOR TODAY` is assumed.

!!! note  
    Temporal clauses operate on calendar days. For example, from midnight `FOR TODAY` will return no data until data is written for that day.

## Single Dates

Data from a specific, single, date can be obtained using the `FOR date` syntax. 

~~~
FOR date
~~~

Date values in `FOR` clauses must either be in 'YYYY-MM-DD' format or a recognised date placeholder, for example.

- `FOR TODAY`
- `FOR YESTERDAY`
- `FOR '2022-02-14'`

## Date Ranges

Data within a range of dates can be specified using `FOR DATES BETWEEN` or `FOR DATES IN` syntax. Where data is retrieved for multiple dates, the datasets for each day have an implicit `UNION ALL` applied to them.

~~~
FOR DATES BETWEEN start AND end
~~~
~~~
FOR DATES IN range
~~~

Date values in `BETWEEN` clauses must either be in 'YYYY-MM-DD' format or a recognised date placeholder, for example:

- `FOR DATES BETWEEN '2000-01-01' AND TODAY`
- `FOR DATES BETWEEN '2020-04-01' AND '2020-04-30'`

Date range values in `IN` clauses must be recognised date range placeholders, for example:

- `FOR DATES IN LAST_MONTH`

## Placeholders

Placeholder  | Applicability   | Description
------------ | --------------- | ------------
`TODAY`      | FOR, BETWEEN    | This calendar day
`YESTERDAY`  | FOR, BETWEEN    | The previous calendar day
`THIS_MONTH` | IN              | Since the first of the current month
`LAST_MONTH` | IN              | The previous calendar month (also `PREVIOUS_MONTH`)

!!! caution  
    - `FOR` clauses cannot contain comments or reference column values or aliases  
    - Dates can not include times and must be in the format 'YYYY-MM-DD'  
    - The default partition scheme does not support Temporal queries  
    - Only one temporal clause can be provided, the same dates will be used for all datasets in the query. If you are performing a `JOIN` or a subquery, the date or date ranges are applied to all datasets in the query.
