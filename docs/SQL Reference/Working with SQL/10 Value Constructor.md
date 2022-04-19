# Value Constructor

## Creating Dummy Tables

### Using `VALUES`

`VALUES` allows you to create a multicolumn dummy, or temporary, table, available until the query has finished executing.

A simple example is as follows:

~~~sql
SELECT * 
  FROM
    (
      VALUES ('High', 3),
             ('Medium', 2),
             ('Low', 1)
    ) AS ratings (name, rating);
~~~

Result:

~~~
 name   | rating
--------+--------
 High   |      3
 Medium |      2
 Low    |      1
~~~
