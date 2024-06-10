import sqlite3

DB_NAME = "sqlite3_opt.db"


def create_table_statement(name: str, schema) -> str:
    yield f"CREATE TABLE IF NOT EXISTS {name} ("
    total_columns = len(schema.columns)
    for i, column in enumerate(schema.columns, start=1):
        # Check if it's the last column
        if i == total_columns:
            yield f"\t{column.name:<20}\t{column.type.name}"
        else:
            yield f"\t{column.name:<20}\t{column.type.name},"
    yield ");"


def format_value(value):
    try:
        if value != value:  # Check for NaN values which are not equal to themselves
            return "NULL"
        elif isinstance(value, str):
            return "'" + value.replace("'", "''") + "'"  # Properly quote strings
    except:
        pass
    return str(value)  # Use the string representation for other data types


def creator(name, con: sqlite3.Connection, dataset):
    create_sql = "\n".join(create_table_statement(name, dataset.schema))
    print(create_sql)
    con.execute(create_sql)

    con.execute("BEGIN")

    for row in dataset:
        con.execute(f'INSERT INTO {name} VALUES ({", ".join(format_value(r) for r in row)});')
    con.commit()


def main():
    conn = sqlite3.connect(DB_NAME, isolation_level=None)
    conn.execute("PRAGMA journal_mode = OFF;")
    conn.execute("PRAGMA synchronous = 0;")
    conn.execute("PRAGMA cache_size = 1000000;")  # give it a GB
    conn.execute("PRAGMA locking_mode = EXCLUSIVE;")
    conn.execute("PRAGMA temp_store = MEMORY;")

    import opteryx

    # dataset = opteryx.query("SELECT tweet_id, text, timestamp, user_id, user_verified, user_name, followers, following, tweets_by_user FROM testdata.flat.formats.parquet;")
    # creator("tweets", conn, dataset)
    # dataset = opteryx.query("SELECT * FROM $planets;")
    # creator("planets", conn, dataset)

    print(conn.execute("SELECT * FROM tweets").fetchall())
    # print(conn.execute("SELECT * FROM planets").fetchall())


if __name__ == "__main__":
    main()
