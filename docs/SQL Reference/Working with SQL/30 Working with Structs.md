# Working with Structs

In Opteryx a struct is a collection of zero or more key, value pairs. Keys must be `VARCHAR`, values can be different types.

## Actions

### Reading

~~~
struct[key]
~~~

Values within structs can be accessed by key using map notation, putting the key in square brackets following the struct.

Example:

~~~sql
SELECT birth_place['town']
  FROM $astronauts
~~~

### Searching

~~~
`SEARCH(struct, value)`
~~~

All values in a struct can be searched for a given value using the `SEARCH` function.

Example:

~~~sql
SELECT name,
       SEARCH(birth_place, 'Italy')
  FROM $astronauts
~~~

## Limitations

Structs have the following limitations

- Statements cannot ORDER BY a struct column
- Statements cannot contain DISTINCT and JOIN when the tables include struct columns
- Structs cannot be used in comparisons

!!! note
    Some restrictions may be resolved by the query optimizer, for example, Projection Pushdown may remove struct columns as part of optimization. However, you should not rely on the optimizer to
    take any particular action.