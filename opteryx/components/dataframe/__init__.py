def _table_to_tuples(table):
    # Get the schema and columns from the table
    schema = table.schema
    columns = [table.column(i) for i in schema.names]
    # Create a list of tuples from the columns
    rows = [tuple(col[i].as_py() for col in columns) for i in range(table.num_rows)]
    return rows
