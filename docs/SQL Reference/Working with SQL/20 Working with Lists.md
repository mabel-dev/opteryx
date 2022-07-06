# Working with Lists

In Opteryx a list is an ordered collection of zero or more values of the same data type.

## Actions

### Accessing

~~~
`list[index]`
~~~

### Testing

~~~
`value IN list`
~~~

## Searching

~~~
`SEARCH(list, value)`
~~~

`LIST_CONTAINS`   
`LIST_CONTAINS_ANY`   
`LIST_CONTAINS_ALL`

## Converting Lists to Relations

### Using `UNNEST`

`UNNEST` allows you to create a single column table either as a list of literals, or from a column of LIST type in a dataset.

~~~sql
SELECT * 
  FROM UNNEST((True, False)) AS Booleans;
~~~