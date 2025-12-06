import os
import json
import sqlite3
import opteryx
import decimal

DB_NAME = "testdata/sqlite/database.db"

os.remove(DB_NAME) if os.path.exists(DB_NAME) else None

def create_table_statement(name: str, schema) -> str:
    yield f"CREATE TABLE IF NOT EXISTS {name} ("
    total_columns = len(schema.columns)
    for i, column in enumerate(schema.columns, start=1):
        column_name = column.name
        if column.name == "group":
            column_name = '"group"'  # Escape reserved keyword
        # Check if it's the last column
        if i == total_columns:
            yield f"\t{column_name:<20}\t{column.type.name}"
        else:
            yield f"\t{column_name:<20}\t{column.type.name},"
    yield ");"


def format_value(value):
    """Convert Python values to SQLite-friendly values for parameter binding.

    Important:
    - Return None for SQL NULLs (Python None or NaN values).
    - Return JSON strings for arrays.
    - Preserve numeric types so SQLite can store numbers as numbers rather than strings.
    - Escape strings so they are stored correctly.
    """
    try:
        # Preserve proper NULLs
        if value is None:
            return None
        # NaN: value != value is a quick check
        if value != value:
            return None
        # Arrays / lists -> JSON
        if isinstance(value, (list, tuple)):
            return json.dumps(value)
        # Strings should be returned as-is (we don't need to double-quote here; parameter binding handles it)
        if isinstance(value, str):
            return value
        # Decimal -> convert to float if possible
        if isinstance(value, decimal.Decimal):
            try:
                return float(value)
            except (decimal.InvalidOperation, OverflowError, ValueError):
                return str(value)
        # Numpy-like objects with tolist
        if hasattr(value, "tolist") and not isinstance(value, (str, bytes)):
            try:
                arr = value.tolist()
            except (AttributeError, TypeError, ValueError):
                arr = None
            if isinstance(arr, (list, tuple)):
                return json.dumps(arr)
    except (TypeError, ValueError):
        pass
    # For numeric types (int/float) and others, return as-is so parameter binding preserves type
    return value


def creator(name, con: sqlite3.Connection, dataset):
    create_sql = "\n".join(create_table_statement(name, dataset.schema))
    print(create_sql)
    con.execute(create_sql)

    con.execute("BEGIN")

    for row in dataset:
        statement = f'INSERT INTO {name} VALUES ({", ".join("?" for _ in row)});'
        values = [format_value(v) for v in row]
        print(statement, tuple(values))
        con.execute(statement, values)
    con.commit()


def main():
    conn = sqlite3.connect(DB_NAME, isolation_level=None)
    conn.execute("PRAGMA journal_mode = OFF;")
    conn.execute("PRAGMA synchronous = 0;")
    conn.execute("PRAGMA cache_size = 1000000;")  # give it a GB
    conn.execute("PRAGMA locking_mode = EXCLUSIVE;")
    conn.execute("PRAGMA temp_store = MEMORY;")


    dataset = opteryx.query("SELECT tweet_id, text, timestamp, user_id, user_verified, user_name, followers, following, tweets_by_user FROM testdata.flat.formats.parquet;")
    creator("tweets", conn, dataset)
    dataset = opteryx.query("SELECT * FROM $planets;")
    creator("planets", conn, dataset)
    dataset = opteryx.query("SELECT * FROM $satellites;")
    creator("satellites", conn, dataset)
    dataset = opteryx.query("SELECT * FROM $astronauts;")
    creator("astronauts", conn, dataset)

    #print(conn.execute("SELECT * FROM astronauts").fetchall())
    # print(conn.execute("SELECT * FROM planets").fetchall())


if __name__ == "__main__":
    main()
