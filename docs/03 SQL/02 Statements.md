# SQL Statements

SELECT [DISTINCT] <select_expression>
FROM <table_list>
WHERE <where_expression>
GROUP BY <group_by_expression>
HAVING <having_expression>
[LIMIT|SAMPLE] <n|ratio>
SKIP <n>
ORDER BY <field_list> [ASC|DESC]


-> row data

~~~sql
ANALYZE TABLE table_name
~~~

->
    Name: DataSet Name
    Format: File Type
    Rows: Row Count  <- from the BRIN
    Blobs: Blob Count
    Bytes: Raw Byte Count
    Columns: List of columns and types

EXPLAIN query -> returns the plan for a query

~~~sql
CREATE INDEX index_name ON dataset.name (columns)
~~~