SELECT [DISTINCT] <select_expression>
FROM <table_list>
WHERE <where_expression>
GROUP BY <group_by_expression>
HAVING <having_expression>
[LIMIT|SAMPLE] <n|ratio>
ORDER BY <field_list> [ASC|DESC]

-> row data

ANALYZE dataset -> creates and/or returns profile information for a dataset
->
    Name: DataSet Name
    Format: File Type
    Rows: Row Count
    Blobs: Blob Count
    Bytes: Raw Byte Count
    Columns: List of columns and types

EXPLAIN [NOOPT] query -> returns the plan for a query

DESCRIBE dataset -> creates and/or returns schema information for a dataset

CREATE [MINMAX|BTREE|BINARY|BITMAP] INDEX index_name ON dataset (attribute1) -> creates an index