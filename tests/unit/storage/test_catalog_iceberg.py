
import os
import sys
import pyarrow

sys.path.insert(1, os.path.join(sys.path[0], "../../.."))

from tests import is_arm, is_mac, is_windows, skip_if
from tests import set_up_iceberg
import opteryx
from opteryx.connectors import DiskConnector
from opteryx.connectors import IcebergConnector
from opteryx.compiled.structures.relation_statistics import to_int


# this is how we get the raw list of files for the scan
# print([task.file.file_path for task in self.table.scan().plan_files()])

@skip_if(is_arm() or is_windows() or is_mac())
def test_iceberg_basic():

    catalog = set_up_iceberg()
    opteryx.register_store(
        "iceberg",
        IcebergConnector,
        catalog=catalog,
        remove_prefix=True,
    )

    table = catalog.load_table("opteryx.tweets")
    table.scan().to_arrow()


@skip_if(is_arm() or is_windows() or is_mac())
def test_iceberg_get_schema():

    catalog = set_up_iceberg()
    opteryx.register_store(
        "iceberg",
        IcebergConnector,
        catalog=catalog,
        io=DiskConnector,
        remove_prefix=True,
    )

    table = catalog.load_table("opteryx.tweets")
    table.schema().as_arrow()


@skip_if(is_arm() or is_windows() or is_mac())
def test_iceberg_get_statistics_manual():

    from opteryx.models import RelationStatistics

    catalog = set_up_iceberg()
    opteryx.register_store(
        "iceberg",
        IcebergConnector,
        catalog=catalog,
        io=DiskConnector,
        remove_prefix=True,
    )

    table = catalog.load_table("opteryx.tweets")
    table.schema().as_arrow()

    stats = RelationStatistics()

    column_names = {col.field_id:col.name for col in table.schema().columns}
    column_types = {col.field_id:col.field_type for col in table.schema().columns}

    files = table.inspect.files()
    stats.record_count = pyarrow.compute.sum(files.column("record_count")).as_py()

    if "distinct_counts" in files.columns:
        for file in files.column("distinct_counts"):
            for k, v in file:
                stats.set_cardinality_estimate[column_names[k]] += v

    if "value_counts" in files.columns:
        for file in files.column("value_counts"):
            for k, v in file:
                stats.add_count(column_names[k], v)

    for file in files.column("lower_bounds"):
        for k, v in file:
            stats.update_lower(column_names[k], IcebergConnector.decode_iceberg_value(v, column_types[k]))

    for file in files.column("upper_bounds"):
        for k, v in file:
            stats.update_upper(column_names[k], IcebergConnector.decode_iceberg_value(v, column_types[k]))

    assert stats.record_count == 100000
    assert stats.lower_bounds[b"followers"] == 0
    assert stats.upper_bounds[b"followers"] == 8266250
    assert stats.lower_bounds[b"user_name"] == to_int("")
    assert stats.upper_bounds[b"user_name"] == to_int("🫖🔫")
    assert stats.lower_bounds[b"tweet_id"] == to_int(1346604539013705728)
    assert stats.upper_bounds[b"tweet_id"] == to_int(1346615999009755142)
    assert stats.lower_bounds[b"text"] == to_int("!! PLEASE STOP A")
    assert stats.upper_bounds[b"text"] == to_int("🪶Cultural approq")
    assert stats.lower_bounds[b"timestamp"] == to_int("2021-01-05T23:48")
    assert stats.upper_bounds[b"timestamp"] == to_int("2021-01-06T00:35")

@skip_if(is_arm() or is_windows() or is_mac())
def test_iceberg_connector():

    catalog = set_up_iceberg()

    opteryx.register_store(
        "iceberg",
        IcebergConnector,
        catalog=catalog,
        remove_prefix=True,
    )
    table = opteryx.query("SELECT * FROM iceberg.opteryx.tweets WHERE followers = 10")
    assert table.shape[0] == 353

@skip_if(is_arm() or is_windows() or is_mac())
def test_iceberg_get_stats_tweets():

    from opteryx.connectors import IcebergConnector, connector_factory

    catalog = set_up_iceberg()

    opteryx.register_store(
        "iceberg",
        IcebergConnector,
        catalog=catalog,
        io=DiskConnector,
        remove_prefix=True,
    )
    connector = connector_factory("iceberg.opteryx.tweets", None)
    connector.get_dataset_schema()
    stats = connector.relation_statistics

    assert stats.record_count == 100000
    assert stats.lower_bounds[b"followers"] == 0
    assert stats.upper_bounds[b"followers"] == 8266250
    assert stats.lower_bounds[b"user_name"] == to_int("")
    assert stats.upper_bounds[b"user_name"] == to_int("🫖🔫")
    assert stats.lower_bounds[b"tweet_id"] == to_int(1346604539013705728)
    assert stats.upper_bounds[b"tweet_id"] == to_int(1346615999009755142)
    assert stats.lower_bounds[b"text"] == to_int("!! PLEASE STOP A")
    assert stats.upper_bounds[b"text"] == to_int("🪶Cultural approq")
    assert stats.lower_bounds[b"timestamp"] == to_int("2021-01-05T23:48")
    assert stats.upper_bounds[b"timestamp"] == to_int("2021-01-06T00:35")
    
@skip_if(is_arm() or is_windows() or is_mac())
def test_iceberg_get_stats_missions():

    from opteryx.connectors import IcebergConnector, connector_factory

    catalog = set_up_iceberg()

    opteryx.register_store(
        "iceberg",
        IcebergConnector,
        catalog=catalog,
        io=DiskConnector,
        remove_prefix=True,
    )
    connector = connector_factory("iceberg.opteryx.tweets", None)
    connector.get_dataset_schema()
    stats = connector.relation_statistics

    assert stats.record_count == 100000
    assert stats.lower_bounds[b"followers"] == 0
    assert stats.upper_bounds[b"followers"] == 8266250
    assert stats.lower_bounds[b"user_name"] == to_int("")
    assert stats.upper_bounds[b"user_name"] == to_int("🫖🔫")
    assert stats.lower_bounds[b"tweet_id"] == to_int(1346604539013705728)
    assert stats.upper_bounds[b"tweet_id"] == to_int(1346615999009755142)
    assert stats.lower_bounds[b"text"] == to_int("!! PLEASE STOP A")
    assert stats.upper_bounds[b"text"] == to_int("🪶Cultural approq")
    assert stats.lower_bounds[b"timestamp"] == to_int("2021-01-05T23:48")
    assert stats.upper_bounds[b"timestamp"] == to_int("2021-01-06T00:35")

@skip_if(is_arm() or is_windows() or is_mac())
def test_iceberg_get_stats_remote():

    from decimal import Decimal
    from pyiceberg.catalog import load_catalog
    from opteryx.connectors import IcebergConnector, connector_factory

    DATA_CATALOG_CONNECTION = os.environ.get("DATA_CATALOG_CONNECTION")
    DATA_CATALOG_STORAGE = os.environ.get("DATA_CATALOG_STORAGE")

    catalog = load_catalog(
        "opteryx",
        **{
            "uri": DATA_CATALOG_CONNECTION,
            "warehouse": DATA_CATALOG_STORAGE,
        }
    )

    opteryx.register_store(
        "iceberg",
        IcebergConnector,
        catalog=catalog,
        remove_prefix=True,
    )
    connector = connector_factory("iceberg.iceberg.planets", None)
    connector.get_dataset_schema()
    stats = connector.relation_statistics

    assert stats.record_count == 9
    assert stats.lower_bounds[b"id"] == 1, stats.lower_bounds[b"id"]
    assert stats.upper_bounds[b"id"] == 9, stats.upper_bounds[b"id"]
    assert stats.lower_bounds[b"name"] == to_int("Earth"), stats.lower_bounds[b"name"]
    assert stats.upper_bounds[b"name"] == to_int("Venus"), stats.upper_bounds[b"name"]
    assert stats.lower_bounds[b"mass"] == to_int(0.0146), stats.lower_bounds[b"mass"]
    assert stats.upper_bounds[b"mass"] == to_int(1898.0), stats.upper_bounds[b"mass"]
    assert stats.lower_bounds[b"diameter"] == 2370, stats.lower_bounds[b"diameter"]
    assert stats.upper_bounds[b"diameter"] == 142984, stats.upper_bounds[b"diameter"]
    assert stats.lower_bounds[b"gravity"] == to_int(Decimal("0.7")), stats.lower_bounds[b"gravity"]
    assert stats.upper_bounds[b"gravity"] == to_int(Decimal("23.1")), stats.upper_bounds[b"gravity"]
    assert stats.lower_bounds[b"surfacePressure"] == to_int(0.0), stats.lower_bounds[b"surfacePressure"]
    assert stats.upper_bounds[b"surfacePressure"] == to_int(92.0), stats.upper_bounds[b"surfacePressure"]


@skip_if(is_arm() or is_windows() or is_mac())
def test_iceberg_remote():

    from pyiceberg.catalog import load_catalog

    DATA_CATALOG_CONNECTION = os.environ.get("DATA_CATALOG_CONNECTION")
    DATA_CATALOG_STORAGE = os.environ.get("DATA_CATALOG_STORAGE")

    catalog = load_catalog(
        "opteryx",
        **{
            "uri": DATA_CATALOG_CONNECTION,
            "warehouse": DATA_CATALOG_STORAGE,
        }
    )

    opteryx.register_store(
        "iceberg",
        IcebergConnector,
        catalog=catalog,
        remove_prefix=True,
    )

    table = opteryx.query("SELECT * FROM iceberg.iceberg.tweets WHERE followers = 10")
    assert table.shape[0] == 353


if __name__ == "__main__":  # pragma: no cover
    from tests import run_tests

    run_tests()
