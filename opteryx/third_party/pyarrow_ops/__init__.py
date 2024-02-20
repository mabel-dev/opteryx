import pyarrow


def align_tables(source_table, append_table, source_indices, append_indices):
    # If either source_indices or append_indices is empty, return the source_table taken with source_indices immediately
    if len(source_indices) == 0 or len(append_indices) == 0:
        # Combine schemas from both tables
        combined_schema = pyarrow.schema([])
        for field in source_table.schema:
            combined_schema = combined_schema.append(field)
        for field in append_table.schema:
            if field.name not in combined_schema.names:
                combined_schema = combined_schema.append(field)

        # Create and return an empty table with the combined schema
        empty_arrays = [pyarrow.array([]) for field in combined_schema]
        return pyarrow.Table.from_arrays(empty_arrays, schema=combined_schema)

    # Take the rows from source_table at the specified source_indices
    aligned_table = source_table.take(source_indices)

    # Create a set of column names from the source table for efficient existence checking
    source_column_names = set(source_table.column_names)

    # Iterate through the column names of append_table
    for column_name in append_table.column_names:
        # If the column_name is not found in source_column_names
        if column_name not in source_column_names:
            # Append the column from append_table to aligned_table, taking the elements at the specified append_indices
            aligned_table = aligned_table.append_column(
                column_name, append_table.column(column_name).take(append_indices)
            )

    # Return the aligned table
    return aligned_table
