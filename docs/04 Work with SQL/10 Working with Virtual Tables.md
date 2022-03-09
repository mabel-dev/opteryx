# Working with Virtual Tables

## Creating Virtual Tables

### Creating with `VALUES`

`VALUES` allows you to create a multicolumn table.

~~~sql
SELECT * 
  FROM
    (
      VALUES ('High', 3),
             ('Medium', 2),
             ('Low', 1)
    ) AS ratings (name, rating)
~~~

### Creating with `UNNEST`

`UNNEST` allows you to create a single column table either as a list of literals, or from a column of LIST type in a dataset.

~~~sql
SELECT * 
  FROM UNNEST((True, False)) AS Booleans
~~~

## Querying Virtual Tables

