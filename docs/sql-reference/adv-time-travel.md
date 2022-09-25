# Time Travel

Opteryx support temporality, the ability to view things as they were at a different point in time.

For datasets which are snapshots, this allows you to recall the data of that snapshop as at a data in the past. For datasets which are logs, this allows you to prune queries to just the dates which contain relevant data.

!!! Note  
    - Data must be Mabel partitioned or using a custom partition schema which supports data partitioning.
    - Data returned for previous days with be the latest data as at today. For example if a backfill updates data from seven days ago, when querying that data today the backfilled data will be returned.
    - There is no implicit deduplication of records as they are returned.


Partition schemes that supports temporal queries allow you to view data from a different date by using a `FOR` clause in the SQL statement. `FOR` clauses state the date, or date range, a query should retrieve results for.
  
If no temporal clause is provided and the schema supports it, `FOR TODAY` is assumed.

!!! Warning     
    Temporal clauses operate on calendar days in UTC. For example, from midnight `FOR TODAY` will return no data until data is written for that day.

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

Date values in `BETWEEN` clauses must either be in 'YYYY-MM-DD' format or a recognized date placeholder, for example:

- `FOR DATES BETWEEN '2000-01-01' AND TODAY`
- `FOR DATES BETWEEN '2020-04-01' AND '2020-04-30'`

Date range values in `IN` clauses must be recognized date range placeholders, for example:

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
    - Only one temporal clause can be provided, the same dates will be used for all datasets in the query. If you are performing a `JOIN` or a sub query, the date or date ranges are applied to all datasets in the query - this may have unintended consequences.


## Time Travel

You can query dates or date ranges using a `FOR` clause in your query. For example to view the contents of partition

~~~sql
SELECT *
  FROM $planets
   FOR YESTERDAY;
~~~

This technique is well suited to viewing snapshotted datasets from a previoud point in time. 

The '$planets' dataset has special handling to respond to temporal queries; Uranus was discovered in 1846 and Pluto was discovered in 1930, we and use the `FOR` clause to query the '$planets' relation from before those planets were discovered like this:

~~~sql
SELECT name
  FROM $planets
   FOR '1846-01-01';
~~~

Returns:

~~~
name
-------
Mercury
Venus
Earth
Mars
Jupiter
Saturn
Neptune
~~~

## Accumulation

For datasets which are continually added to, such as logs, the `FOR` clause can be used to quickly filter ranges of records to search over. The `FOR` clause will most likely record the date the record was written (the 'SYSTEM_TIME' for the record) which may not be the same as the logical or effective date for a record, especially in situations where there is a lag in the records being recorded.

The `BETWEEN` keyword can be used to describe ranges of records, this is useful for querying logged data between two dates.

~~~sql
SELECT name
  FROM $planets
   FOR DATES BETWEEN '2021-01-01' and '2022-12-31';
~~~
