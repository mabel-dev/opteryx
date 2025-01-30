
import os
import sys
import pyarrow

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

from tests.tools import is_arm, is_mac, is_windows, skip_if
from tests.tools import set_up_iceberg
import opteryx
from opteryx.connectors import DiskConnector
from opteryx.connectors import IcebergConnector


# this is how we get the raw list of files for the scan
# print([task.file.file_path for task in self.table.scan().plan_files()])

@skip_if(is_arm() or is_windows() or is_mac())
def test_iceberg_basic():

    catalog = set_up_iceberg()
    opteryx.register_store("iceberg", IcebergConnector, catalog=catalog)

    table = catalog.load_table("iceberg.tweets")
    table.scan().to_arrow()


@skip_if(is_arm() or is_windows() or is_mac())
def test_iceberg_get_schema():

    catalog = set_up_iceberg()
    opteryx.register_store("iceberg", IcebergConnector, catalog=catalog, io=DiskConnector)

    table = catalog.load_table("iceberg.tweets")
    table.schema().as_arrow()


@skip_if(is_arm() or is_windows() or is_mac())
def test_iceberg_get_statistics_manual():

    from opteryx.models.relation_statistics import RelationStatistics

    catalog = set_up_iceberg()
    opteryx.register_store("iceberg", IcebergConnector, catalog=catalog, io=DiskConnector)

    table = catalog.load_table("iceberg.tweets")
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
    assert stats.lower_bounds["followers"] == 0
    assert stats.upper_bounds["followers"] == 8266250
    assert stats.lower_bounds["user_name"] == ""
    assert stats.upper_bounds["user_name"] == "🫖🔫"
    assert stats.lower_bounds["tweet_id"] == 1346604539013705728
    assert stats.upper_bounds["tweet_id"] == 1346615999009755142
    assert stats.lower_bounds["text"] == "!! PLEASE STOP A"
    assert stats.upper_bounds["text"] == "🪶Cultural approq"
    assert stats.lower_bounds["timestamp"] == "2021-01-05T23:48"
    assert stats.upper_bounds["timestamp"] == "2021-01-06T00:35"

@skip_if(is_arm() or is_windows() or is_mac())
def test_iceberg_connector():

    catalog = set_up_iceberg()

    opteryx.register_store("iceberg", IcebergConnector, catalog=catalog)
    table = opteryx.query("SELECT * FROM iceberg.tweets WHERE followers = 10")
    assert table.shape[0] == 353

@skip_if(is_arm() or is_windows() or is_mac())
def test_iceberg_get_stats_tweets():

    from opteryx.connectors import IcebergConnector, connector_factory

    catalog = set_up_iceberg()

    opteryx.register_store("iceberg", IcebergConnector, catalog=catalog, io=DiskConnector)
    connector = connector_factory("iceberg.tweets", None)
    connector.get_dataset_schema()
    stats = connector.relation_statistics

    assert stats.record_count == 100000
    assert stats.lower_bounds["followers"] == 0
    assert stats.upper_bounds["followers"] == 8266250
    assert stats.lower_bounds["user_name"] == ""
    assert stats.upper_bounds["user_name"] == "🫖🔫"
    assert stats.lower_bounds["tweet_id"] == 1346604539013705728
    assert stats.upper_bounds["tweet_id"] == 1346615999009755142
    assert stats.lower_bounds["text"] == "!! PLEASE STOP A"
    assert stats.upper_bounds["text"] == "🪶Cultural approq"
    assert stats.lower_bounds["timestamp"] == "2021-01-05T23:48"
    assert stats.upper_bounds["timestamp"] == "2021-01-06T00:35"
    
@skip_if(is_arm() or is_windows() or is_mac())
def test_iceberg_get_stats_missions():

    from opteryx.connectors import IcebergConnector, connector_factory

    catalog = set_up_iceberg()

    opteryx.register_store("iceberg", IcebergConnector, catalog=catalog, io=DiskConnector)
    connector = connector_factory("iceberg.tweets", None)
    connector.get_dataset_schema()
    stats = connector.relation_statistics

    assert stats.record_count == 100000
    assert stats.lower_bounds["followers"] == 0
    assert stats.upper_bounds["followers"] == 8266250
    assert stats.lower_bounds["user_name"] == ""
    assert stats.upper_bounds["user_name"] == "🫖🔫"
    assert stats.lower_bounds["tweet_id"] == 1346604539013705728
    assert stats.upper_bounds["tweet_id"] == 1346615999009755142
    assert stats.lower_bounds["text"] == "!! PLEASE STOP A"
    assert stats.upper_bounds["text"] == "🪶Cultural approq"
    assert stats.lower_bounds["timestamp"] == "2021-01-05T23:48"
    assert stats.upper_bounds["timestamp"] == "2021-01-06T00:35"

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

    opteryx.register_store("iceberg", IcebergConnector, catalog=catalog)
    connector = connector_factory("iceberg.planets", None)
    connector.get_dataset_schema()
    stats = connector.relation_statistics

    assert stats.record_count == 9
    assert stats.lower_bounds["id"] == 1, stats.lower_bounds["id"]
    assert stats.upper_bounds["id"] == 9, stats.upper_bounds["id"]
    assert stats.lower_bounds["name"] == "Earth", stats.lower_bounds["name"]
    assert stats.upper_bounds["name"] == "Venus", stats.upper_bounds["name"]
    assert stats.lower_bounds["mass"] == 0.0146, stats.lower_bounds["mass"]
    assert stats.upper_bounds["mass"] == 1898.0, stats.upper_bounds["mass"]
    assert stats.lower_bounds["diameter"] == 2370, stats.lower_bounds["diameter"]
    assert stats.upper_bounds["diameter"] == 142984, stats.upper_bounds["diameter"]
    assert stats.lower_bounds["gravity"] == Decimal("0.7"), stats.lower_bounds["gravity"]
    assert stats.upper_bounds["gravity"] == Decimal("23.1"), stats.upper_bounds["gravity"]
    assert stats.lower_bounds["surfacePressure"] == 0.0, stats.lower_bounds["surfacePressure"]
    assert stats.upper_bounds["surfacePressure"] == 92.0, stats.upper_bounds["surfacePressure"]


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

    opteryx.register_store("iceberg", IcebergConnector, catalog=catalog)

    table = opteryx.query("SELECT * FROM iceberg.tweets WHERE followers = 10")
    assert table.shape[0] == 353


if __name__ == "__main__":  # pragma: no cover
    from tests.tools import run_tests

    run_tests()
