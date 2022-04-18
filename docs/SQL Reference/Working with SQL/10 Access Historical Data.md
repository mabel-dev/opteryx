# Accessing Historical Data

Opteryx lets you access Mabel partitioned data as at date or date range in the past.

For datasets which are snapshots, this allows you to recall the data of that snapshop as at a data in the past. For datasets which are logs, this allows you to prune queries to just the dates which contain relevant data.

!!! note
    - Data must be Mabel partitioned or using a custom partition schema which supports data partitioning.
    - Data returned for previous days with be the latest data as at today. For example if a backfill updates data from seven days ago, when querying that data today the backfilled data will be returned.
    - There is no implicit deduplication of records as they are returned.

## Time Travel

You can query dates or date ranges using a `FOR` clause in your query. For example to view the contents of partition

~~~sql
SELECT *
  FROM $planets
   FOR YESTERDAY;
~~~

This technique is well suited to viewing snapshotted datasets from a previoud point in time. 

Uranus was discovered in 1846 and Pluto was discovered in 1930, we could use the `FOR` clause to query the $planets relation from before that date like this

~~~sql
SELECT name
  FROM $planets
   FOR '1846-11-12';
~~~

Which would return

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

!!! note
    The above query is illustrative only demonstrate the concept and do not return these results when actually run.

## Accumulation

For datasets which are continually added to, such as logs, the `FOR` clause can be used to quickly filter ranges of records to search over. The `FOR` clause will most likely record the date the record was written (the SYSTEM_TIME for the record) which may not be the same as the logical or effective date for a record, especially in situations where there is a lag in the records being recorded.

The `BETWEEN` keyword can be used to describe ranges of records,  

~~~sql
SELECT name
  FROM $planets
   FOR DATES BETWEEN '2021-01-01' and '2022-12-31';
~~~

!!! note
    The above query is illustrative only demonstrate the concept and do not return the described results when actually run.