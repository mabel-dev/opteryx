# Working with Lists

In Opteryx a list is an ordered collection of zero or more `VARCHAR` values.

## Actions

### Accessing

~~~
list[index]
~~~

### Testing

~~~
value IN list
~~~

### Searching

~~~
SEARCH(list, value)
~~~
~~~
LIST_CONTAINS   
~~~
~~~
LIST_CONTAINS_ANY   
~~~
~~~
LIST_CONTAINS_ALL
~~~

## Converting Lists to Relations

### Using `UNNEST`

`UNNEST` allows you to create a single column table either as a list of literals, or from a column of LIST type in a dataset.

~~~sql
SELECT * 
  FROM UNNEST((True, False)) AS Booleans;
~~~

## Limitations

Lists have the following limitations

- Statements cannot `ORDER BY` a list column
- Statements cannot contain `DISTINCT` and `JOIN` when the relations include list columns
- Lists cannot be used in comparisons

!!! note
    Some restrictions may be resolved by the query optimizer, for example, Projection Pushdown may remove list columns as part of optimization. However, you should not rely on the optimizer to
    take any particular action.