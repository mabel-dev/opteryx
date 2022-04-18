# SQL Data Types

Opteryx uses a reduced set of types compared to full RDBMS platforms.

## Types

Name        | Description
----------- | ------------------------------------------
`BOOLEAN`   | Logical boolean (True/False).
`NUMERIC`   | All numeric types
`LIST`      | An ordered sequence of data values.
`VARCHAR`   | Variable-length character string.
`STRUCT`    | A dictionary of multiple named values, where each key is a string, but the value can be a different type for each key.
`TIMESTAMP` | Combination of date and time.
`OTHER`     | None of the above or multiple types in the same column. 

!!! note
    OTHER is not a type, it is a catch-all when a type cannot be determined.

## Casting

Values can be cast using the `CAST` function, its form is `CAST(any AS type)`.

## Coercion

### Timestamps

Literal values in quotes may be in interpretted as a `TIMESTAMP` when they match a valid date in ISO 8601 format (e.g. `YYYY-MM-DD` and `YYYY-MM-DD HH:MM`).

### Numbers

All numeric values are coerced to 64bit Floats.