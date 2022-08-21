# Data Types

[Opteryx](https://github.com/mabel-dev/opteryx) uses a reduced set of types compared to full RDBMS platforms.

## Types

Name        | Symbol                       | Description
----------- | :--------------------------: | --------------
`BOOLEAN`   | :fontawesome-solid-check:    | Logical boolean (True/False).
`NUMERIC`   | :fontawesome-solid-hashtag:  | All numeric types.
`LIST`      | :fontawesome-solid-bars:     | An ordered sequence of strings.
`VARCHAR`   | :fontawesome-solid-a:        | Variable-length character string.
`STRUCT`    | :fontawesome-solid-box:      | A dictionary of multiple named values, where each key is a string, but the value can be a different type for each key.
`TIMESTAMP` | :fontawesome-regular-clock:  | Combination of date and time.
`INTERVAL`  | :fontawesome-solid-arrows-left-right-to-line: | The difference between two TIMESTAMP values
`OTHER`     | :fontawesome-solid-question: | None of the above or multiple types in the same column. 

!!! note
    - `INTERVAL` has limited support.
    - `OTHER` is not a type, it is a catch-all when a type cannot be determined.

## Casting

Values can be cast using the `CAST` function, its form is `CAST(any AS type)`. Where values are incompatible, an error will be thrown, to avoid errors `TRY_CAST` can be used instead which will return `NULL` instead of error.

## Coercion

### Timestamps

Literal values in quotes may be in interpreted as a `TIMESTAMP` when they match a valid date in [ISO 8601](https://www.iso.org/iso-8601-date-and-time-format.html)  format (e.g. `YYYY-MM-DD` and `YYYY-MM-DD HH:MM`).

All `TIMESTAMP` and date values read from datasets are coerced to nanosecond precision timestamps.

### Numbers

All numeric values included in SQL statements and read from datasets are coerced to 64bit floats.