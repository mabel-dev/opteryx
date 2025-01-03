
import os
import opteryx
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.catalog import load_catalog

BASE_PATH: str = "tmp/iceberg"

def set_up_iceberg():
    """
    Set up a local Iceberg catalog for testing with NVD data.

    Parameters:
        parquet_files: List[str]
            List of paths to Parquet files partitioned by CVE date.
        base_path: str, optional
            Path to create the Iceberg warehouse, defaults to '/tmp/iceberg_nvd'.

    Returns:
        str: Path to the created Iceberg table.
    """
    # Clean up previous test runs if they exist
    if os.path.exists(BASE_PATH):
        import shutil
        shutil.rmtree(BASE_PATH)
    os.makedirs(BASE_PATH, exist_ok=True)

    # Step 1: Create a local Iceberg catalog
    catalog = SqlCatalog(
        "default",
        **{
            "uri": f"sqlite:///{BASE_PATH}/pyiceberg_catalog.db",
            "warehouse": f"file://{BASE_PATH}",
        },
    )

    # Step 2: Get the data (so we can get the schema)
    data = opteryx.query_to_arrow("SELECT * FROM testdata.flat.formats.parquet")

    # Step 3: Create an Iceberg table
    catalog.create_namespace("iceberg")
    table = catalog.create_table("iceberg.tweets", schema=data.schema)

    # Step 4: Copy the Parquet files into the warehouse
    table.overwrite(data)

    print(f"Iceberg table set up at {BASE_PATH}")
    return BASE_PATH

set_up_iceberg()

catalog = load_catalog(
        "default",
        **{
            "uri": f"sqlite:///{BASE_PATH}/pyiceberg_catalog.db",
            "warehouse": f"file://{BASE_PATH}",
        },
)

table = catalog.load_table("iceberg.tweets")
print(table.scan().to_arrow())