# Temporality

Partition schemes that supports temporal queries allow you to view data from a different time by using a `FOR` clause in the SQL statement. `FOR` clauses state the date, or date range, a query should retrieve results for.

Where a date range is provided, the datasets for each day have an implicit `UNION` applied to them.

## Valid Clauses

Clause               | Description                           | Example
-------------------- | ------------------------------------- | ---------------------------
`FOR 'date'`         | Data as at a specific date            | `FOR '2022-09-16'`
`FOR DATES BETWEEN 'date' AND 'date'` | Data between two specific dates | `FOR DATES BETWEEN '2000-01-01' AND '2000-12-31'`  
`FOR DATES IN range` | Data between a defined range          | `FOR DATES IN LAST_MONTH`

Date values in `FOR` clauses must either be in 'YYYY-MM-DD' format or a recognised date placeholder:

- `TODAY`
- `YESTERDAY`

for example: `FOR DATES BETWEEN '2000-01-01' AND TODAY` or `FOR TODAY`

Date Values in `FOR IN` clauses must use month range values:

Supported range values with `FOR IN` are:

- `THIS_MONTH` = since the first of the current month
- `LAST_MONTH` = the previous calendar month (also `PREVIOUS_MONTH`)

for example : `FOR DATES IN THIS MONTH`

!!! note
    If no temporal clause is provided, `FOR TODAY` is assumed.

!!! caution
    - `FOR` clauses cannot contain comments or reference column values or aliases
    - Dates can not include times and must be in the format 'YYYY-MM-DD'
    - The default partition scheme does not support Temporal queries
    - Only one temporal clause can be provided, the same dates will be used for all datasets in the query
