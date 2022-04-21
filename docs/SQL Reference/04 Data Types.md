# Data Types

Opteryx uses a reduced set of types compared to full RDBMS platforms.

## Types

Name        | Icon                         | Description
----------- | ---------------------------- | --------------
`BOOLEAN`   | :fontawesome-solid-check:    | Logical boolean (True/False).
`NUMERIC`   | :fontawesome-solid-hashtag:  | All numeric types
`LIST`      | :fontawesome-solid-bars:     | An ordered sequence of data values.
`VARCHAR`   | :fontawesome-solid-a:        | Variable-length character string.
`STRUCT`    | :fontawesome-solid-box:      | A dictionary of multiple named values, where each key is a string, but the value can be a different type for each key.
`TIMESTAMP` | :fontawesome-regular-clock:  | Combination of date and time.
`OTHER`     | :fontawesome-solid-question: | None of the above or multiple types in the same column. 

!!! note
    OTHER is not a type, it is a catch-all when a type cannot be determined.

## Casting

Values can be cast using the `CAST` function, its form is `CAST(any AS type)`.

## Coercion

### Timestamps

Literal values in quotes may be in interpretted as a `TIMESTAMP` when they match a valid date in ISO 8601 format (e.g. `YYYY-MM-DD` and `YYYY-MM-DD HH:MM`).

### Numbers

All numeric values are coerced to 64bit Floats.