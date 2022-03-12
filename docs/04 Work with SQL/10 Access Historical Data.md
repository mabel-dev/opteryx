# Access Historical Data

Opteryx lets you access Mabel partitioned data as at date or date range in the past.

For datasets which are snapshots, this allows you to recall the data of that snapshop as at a data in the past. For datasets which are logs, this allows you to prune queries to just the dates which contain relevant data.

## Limitations

- Data must be Mabel partitioned or using a custom partition schema which supports data partitioning.
- Data returned for previous days with be the latest data as at today. For example if a backfill updates data from seven days ago, when querying that data today the backfilled data will be returned.
- There is no implicit deduplication of records as they are returned.

## Time Travel

You can query dates or date ranges using a `FOR` clause in your query. For example to view the contents of partition

~~~sql
SELECT *
  FROM my_table
   FOR YESTERDAY
~~~