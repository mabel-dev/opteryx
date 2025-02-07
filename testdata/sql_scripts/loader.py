"""
Script to execute an SQL file for initial data loading using SQLAlchemy.
"""

from sqlalchemy import create_engine, text

# Configuration - Update these values
DATABASE_URL = ""
SQL_FILE = "testdata/sql_scripts/postgres.sql"  # Path to your SQL file

def execute_sql_script(sql_file: str, db_url: str):
    """
    Execute an SQL script file against the given database.

    Parameters:
        sql_file (str): Path to the SQL file.
        db_url (str): SQLAlchemy database connection URL.
    """
    try:
        # Create database connection
        engine = create_engine(db_url)
        
        with engine.connect() as connection:
            with open(sql_file, "r", encoding="utf-8") as file:
                sql_commands = file.read()
            
            # Execute script
            connection.execute(text(sql_commands))
            connection.commit()  # Commit transaction

        print(f"Successfully executed {sql_file}")

    except Exception as e:
        print(f"Error executing SQL script: {e}")

if __name__ == "__main__":
    execute_sql_script(SQL_FILE, DATABASE_URL)