
import os
import sys
import pyarrow
import pytest

sys.path.insert(1, os.path.join(sys.path[0], "../../.."))

from tests import is_arm, is_mac, is_windows, skip_if
from tests import set_up_iceberg
import opteryx
from opteryx.connectors import IcebergConnector
from opteryx.compiled.structures.relation_statistics import to_int
from opteryx.exceptions import DatasetReadError, UnsupportedSyntaxError
from opteryx.models import QueryStatistics
import datetime
from freezegun import freeze_time

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
        remove_prefix=True,
    )

    connector = connector_factory("iceberg.opteryx.tweets", QueryStatistics())
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
        remove_prefix=True,
    )
    connector = connector_factory("iceberg.opteryx.tweets", QueryStatistics())
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
    connector = connector_factory("iceberg.iceberg.planets", QueryStatistics())
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


@skip_if(is_arm() or is_windows() or is_mac())
def test_iceberg_empty_table_read():
    """
    Tests that an Iceberg table created with no data behaves correctly:
    - Read without time travel returns an empty result with expected columns
    - Time-travel (FOR clause) raises DatasetReadError because there are no snapshots
    """

    catalog = set_up_iceberg()
    opteryx.register_store(
        "iceberg",
        IcebergConnector,
        catalog=catalog,
        remove_prefix=True,
    )

    import pyarrow as pa

    # Create/replace an empty table with a simple schema and no snapshots
    schema = pa.schema([pa.field("id", pa.int64()), pa.field("name", pa.string())])
    table_name = "opteryx.empty_test"
    try:
        catalog.drop_table(table_name)
    except Exception:
        pass
    table = catalog.create_table(table_name, schema=schema)

    # Non-timetravel read should return an empty table with a valid schema
    result = opteryx.query_to_arrow("SELECT * FROM iceberg.opteryx.empty_test")
    assert result.shape == (0, len(schema))

    # Timetravel should raise DatasetReadError because there are no snapshots
    with pytest.raises(DatasetReadError):
        opteryx.query_to_arrow("SELECT * FROM iceberg.opteryx.empty_test FOR '1970-01-01 00:00:00'")


@skip_if(is_arm() or is_windows() or is_mac())
def test_iceberg_empty_table_count_and_schema():
    """
    Verify that empty tables return a valid schema and COUNT(*) returns 0.
    """

    catalog = set_up_iceberg()
    opteryx.register_store(
        "iceberg",
        IcebergConnector,
        catalog=catalog,
        remove_prefix=True,
    )

    import pyarrow as pa

    schema = pa.schema([pa.field("id", pa.int64()), pa.field("name", pa.string())])
    table_name = "opteryx.empty_test"
    try:
        catalog.drop_table(table_name)
    except Exception:
        pass
    catalog.create_table(table_name, schema=schema)

    result = opteryx.query_to_arrow("SELECT * FROM iceberg.opteryx.empty_test")
    assert result.shape == (0, len(schema))
    # Check the schema column names as expected
    assert result.column_names == ["id", "name"]

    count_result = opteryx.query_to_arrow("SELECT COUNT(*) FROM iceberg.opteryx.empty_test;")
    assert count_result.shape == (1, 1)
    # Ensure the COUNT(*) value is 0
    assert count_result.to_pandas().iloc[0, 0] == 0


@skip_if(is_arm() or is_windows() or is_mac())
def test_iceberg_single_snapshot_timetravel():
    """
    Tests that a single-snapshot table honors timetravel semantics.
    - Read without time travel returns the snapshot rows
    - Read for a date before the snapshot returns that snapshot
    - Read for a date after the single snapshot raises DatasetReadError
    """

    catalog = set_up_iceberg()
    opteryx.register_store(
        "iceberg",
        IcebergConnector,
        catalog=catalog,
        remove_prefix=True,
    )

    import pyarrow as pa

    schema = pa.schema([pa.field("id", pa.int64()), pa.field("name", pa.string())])
    # Create/replace a single snapshot table to ensure a clean state
    table_name = "opteryx.single_snapshot"
    try:
        catalog.drop_table(table_name)
    except Exception:
        pass
    table = catalog.create_table(table_name, schema=schema)

    # Prepare a small snapshot
    snapshot_data = pa.Table.from_arrays([pa.array([1, 2, 3]), pa.array(["a", "b", "c"])], schema=schema)
    commit_time = datetime.datetime(2022, 1, 1, 12, 0, 0)
    with freeze_time(commit_time):
        table.append(snapshot_data)

    # Non-time travel read
    result = opteryx.query_to_arrow("SELECT * FROM iceberg.opteryx.single_snapshot")
    assert result.shape[0] == 3

    # Date earlier than snapshot should raise DatasetReadError
    earlier_date = "2020-01-01 00:00:00"
    with pytest.raises(DatasetReadError):
        opteryx.query_to_arrow(f"SELECT * FROM iceberg.opteryx.single_snapshot FOR '{earlier_date}'")

    # Date exactly at commit time should also work
    exact = commit_time.isoformat()
    result = opteryx.query_to_arrow(f"SELECT * FROM iceberg.opteryx.single_snapshot FOR '{exact}'")
    assert result.shape[0] == 3

    # Future date beyond the snapshot should return the latest snapshot
    res = opteryx.query_to_arrow("SELECT * FROM iceberg.opteryx.single_snapshot FOR '2040-01-01 00:00:00'")
    assert res.shape[0] == 3


@skip_if(is_arm() or is_windows() or is_mac())
def test_iceberg_future_read_returns_current_planets():
    """
    A future FOR timestamp beyond the latest snapshot should return current data.
    """
    catalog = set_up_iceberg()
    opteryx.register_store(
        "iceberg",
        IcebergConnector,
        catalog=catalog,
        remove_prefix=True,
    )

    now_res = opteryx.query_to_arrow("SELECT COUNT(*) FROM iceberg.opteryx.planets")
    future_res = opteryx.query_to_arrow("SELECT COUNT(*) FROM iceberg.opteryx.planets FOR '2100-01-01 00:00:00'")
    assert now_res.to_pandas().iloc[0, 0] == future_res.to_pandas().iloc[0, 0]


@skip_if(is_arm() or is_windows() or is_mac())
def test_iceberg_empty_table_select_columns():
    catalog = set_up_iceberg()
    opteryx.register_store(
        "iceberg",
        IcebergConnector,
        catalog=catalog,
        remove_prefix=True,
    )

    import pyarrow as pa
    schema = pa.schema([pa.field("id", pa.int64()), pa.field("name", pa.string())])
    table_name = "opteryx.empty_test"
    try:
        catalog.drop_table(table_name)
    except Exception:
        pass
    catalog.create_table(table_name, schema=schema)

    res = opteryx.query_to_arrow("SELECT id FROM iceberg.opteryx.empty_test")
    assert res.shape == (0, 1)
    assert res.column_names == ["id"]


@skip_if(is_arm() or is_windows() or is_mac())
def test_iceberg_single_snapshot_where_clause():
    catalog = set_up_iceberg()
    opteryx.register_store(
        "iceberg",
        IcebergConnector,
        catalog=catalog,
        remove_prefix=True,
    )

    import pyarrow as pa
    schema = pa.schema([pa.field("id", pa.int64()), pa.field("name", pa.string())])
    table_name = "opteryx.single_snapshot"
    try:
        catalog.drop_table(table_name)
    except Exception:
        pass
    table = catalog.create_table(table_name, schema=schema)
    snapshot_data = pa.Table.from_arrays([pa.array([1, 2, 3]), pa.array(["a", "b", "c"])], schema=schema)
    commit_time = datetime.datetime(2022, 1, 1, 12, 0, 0)
    from freezegun import freeze_time
    from opteryx.exceptions import DatasetReadError

    with freeze_time(commit_time):
        table.append(snapshot_data)

    res = opteryx.query_to_arrow("SELECT * FROM iceberg.opteryx.single_snapshot WHERE id = 1")
    assert res.shape[0] == 1


@skip_if(is_arm() or is_windows() or is_mac())
def test_iceberg_planets_first_snapshot_and_future():
    catalog = set_up_iceberg()
    opteryx.register_store(
        "iceberg",
        IcebergConnector,
        catalog=catalog,
        remove_prefix=True,
    )

    # Date equal to first snapshot should work
    res = opteryx.query_to_arrow("SELECT * FROM iceberg.opteryx.planets FOR '1781-04-25 00:00:00'")
    assert res.shape[0] == 6

    # Date after the latest snapshot should return current data
    res2 = opteryx.query_to_arrow("SELECT * FROM iceberg.opteryx.planets FOR '2100-01-01 00:00:00'")
    regular = opteryx.query_to_arrow("SELECT * FROM iceberg.opteryx.planets")
    assert res2.shape[0] == regular.shape[0]


@skip_if(is_arm() or is_windows() or is_mac())
def test_iceberg_two_snapshot_timetravel_and_join():
    catalog = set_up_iceberg()
    opteryx.register_store(
        "iceberg",
        IcebergConnector,
        catalog=catalog,
        remove_prefix=True,
    )

    # Current state: should have 3 rows
    current = opteryx.query_to_arrow("SELECT * FROM iceberg.opteryx.two_snap_battery")
    assert current.shape[0] == 3

    # Early snapshot should have 2 rows
    early = opteryx.query_to_arrow("SELECT * FROM iceberg.opteryx.two_snap_battery FOR '2021-01-01 12:00:00'")
    assert early.shape[0] == 2

    # Before our first snapshot, we should error
    with pytest.raises(DatasetReadError):
        opteryx.query_to_arrow("SELECT * FROM iceberg.opteryx.two_snap_battery FOR '2020-01-01 00:00:00'")

    # Future date beyond latest should return current
    future = opteryx.query_to_arrow("SELECT * FROM iceberg.opteryx.two_snap_battery FOR '2100-01-01 00:00:00'")
    assert future.shape[0] == current.shape[0]

    # Join differences between snapshots should show rows new in the later snapshot
    diff = opteryx.query_to_arrow("SELECT later.id FROM iceberg.opteryx.two_snap_battery FOR '2022-01-01 12:00:00' AS later LEFT JOIN iceberg.opteryx.two_snap_battery FOR '2021-01-01 12:00:00' AS early ON later.id = early.id WHERE early.id IS NULL")
    assert diff.shape[0] == 1


@skip_if(is_arm() or is_windows() or is_mac())
def test_iceberg_two_snapshot_inspection_and_contents():
    catalog = set_up_iceberg()
    # Load the table and inspect snapshot history
    table = catalog.load_table("opteryx.two_snap_battery")
    snapshots = table.inspect.snapshots().sort_by("committed_at").to_pylist()
    assert len(snapshots) >= 2

    # Verify first snapshot rows
    first_snapshot_id = snapshots[0]["snapshot_id"]
    first_snapshot = table.snapshot_by_id(first_snapshot_id)
    assert first_snapshot is not None
    first_scan = table.scan(snapshot_id=first_snapshot_id).to_arrow()
    assert first_scan.num_rows == 2

    # Verify second snapshot rows
    second_snapshot_id = snapshots[-1]["snapshot_id"]
    second_snapshot = table.snapshot_by_id(second_snapshot_id)
    second_scan = table.scan(snapshot_id=second_snapshot_id).to_arrow()
    assert second_scan.num_rows == 3


@skip_if(is_arm() or is_windows() or is_mac())
def test_iceberg_single_snapshot_date_only_for_unsupported():
    """
    Using a date-only FOR clause should raise UnsupportedSyntaxError because
    the connector only supports point-in-time reads (full timestamp).
    """
    catalog = set_up_iceberg()
    opteryx.register_store(
        "iceberg",
        IcebergConnector,
        catalog=catalog,
        remove_prefix=True,
    )

    import pyarrow as pa
    table_name = "opteryx.single_snapshot_date_only"
    try:
        catalog.drop_table(table_name)
    except Exception:
        pass

    schema = pa.schema([pa.field("id", pa.int64()), pa.field("name", pa.string())])
    table = catalog.create_table(table_name, schema=schema)
    snapshot_data = pa.Table.from_arrays([pa.array([1]), pa.array(["a"])], schema=schema)
    commit_time = datetime.datetime(2022, 1, 1, 12, 0, 0)
    with freeze_time(commit_time):
        table.append(snapshot_data)

    with pytest.raises(UnsupportedSyntaxError):
        opteryx.query_to_arrow(f"SELECT * FROM iceberg.{table_name} FOR '2022-01-01'")


if __name__ == "__main__":  # pragma: no cover
    from tests import run_tests

    run_tests()
