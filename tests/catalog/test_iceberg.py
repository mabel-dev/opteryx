
import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

from tests.tools import is_arm, is_mac, is_windows, skip_if
import opteryx
from opteryx.connectors import DiskConnector
from opteryx.connectors import IcebergConnector

BASE_PATH: str = "tmp/iceberg"

@skip_if(is_arm() or is_windows() or is_mac())
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

    from pyiceberg.catalog.sql import SqlCatalog

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


def test_iceberg_basic():

    from pyiceberg.catalog import load_catalog

    set_up_iceberg()

    catalog = load_catalog(
            "default",
            **{
                "uri": f"sqlite:///{BASE_PATH}/pyiceberg_catalog.db",
                "warehouse": f"file://{BASE_PATH}",
            },
    )

    opteryx.register_store("iceberg", IcebergConnector, io=DiskConnector)

    table = catalog.load_table("iceberg.tweets")
    print(table.scan().to_arrow())


if __name__ == "__main__":  # pragma: no cover
    from tests.tools import run_tests

    run_tests()
