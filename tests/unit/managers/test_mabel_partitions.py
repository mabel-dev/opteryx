
import datetime
import os
import sys

# Add the project root to the path so we can import opteryx
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../")))

from opteryx.managers.schemes.mabel_partitions import MabelPartitionScheme

# Mock blob_list_getter
def mock_blob_list_getter(prefix):
    # prefix format: .../year_YYYY/month_MM/day_DD
    # We assume the prefix ends with the date path
    
    # Extract date from prefix
    parts = prefix.split(os.sep)
    try:
        # parts[-1] = day_DD
        # parts[-2] = month_MM
        # parts[-3] = year_YYYY
        year = int(parts[-3].split('_')[1])
        month = int(parts[-2].split('_')[1])
        day = int(parts[-1].split('_')[1])
    except:
        return []

    blobs = []
    # Generate blobs for all 24 hours for this day
    for h in range(24):
        # Format: .../by_hour/hour=HH/as_at_.../data.parquet
        blob = f"{prefix}{os.sep}by_hour{os.sep}hour={h:02d}{os.sep}as_at_1{os.sep}data.parquet"
        blobs.append(blob)
        # Add complete file
        blobs.append(f"{prefix}{os.sep}by_hour{os.sep}hour={h:02d}{os.sep}as_at_1{os.sep}frame.complete")
    
    return blobs

def _check_results(blobs, expected_hours):
    found_hours = set()
    for blob in blobs:
        # bucket/dataset/year_2023/month_01/day_01/by_hour/hour=21/as_at_1/data.parquet
        parts = blob.split(os.sep)
        year = int(parts[-7].split('_')[1])
        month = int(parts[-6].split('_')[1])
        day = int(parts[-5].split('_')[1])
        hour = int(parts[-3].split('=')[1])
        found_hours.add((year, month, day, hour))
        
    assert len(found_hours) == len(expected_hours), f"Expected {len(expected_hours)} hours, got {len(found_hours)}"
    
    for h in expected_hours:
        assert h in found_hours, f"Missing hour {h}"

def test_mabel_partition_scheme_hourly_cycling():
    scheme = MabelPartitionScheme()
    
    # Test case: Range spanning two days, starting late in the first day and ending early in the second day
    start_date = datetime.datetime(2023, 1, 1, 21, 0, 0)
    end_date = datetime.datetime(2023, 1, 2, 1, 0, 0)
    
    blobs = scheme.get_blobs_in_partition(
        blob_list_getter=mock_blob_list_getter,
        prefix="bucket/dataset",
        start_date=start_date,
        end_date=end_date
    )
    
    expected_hours = [
        (2023, 1, 1, 21),
        (2023, 1, 1, 22),
        (2023, 1, 1, 23),
        (2023, 1, 2, 0),
        (2023, 1, 2, 1),
    ]
    _check_results(blobs, expected_hours)

def test_mabel_partition_scheme_same_day():
    scheme = MabelPartitionScheme()
    
    # Test case: Range within the same day
    start_date = datetime.datetime(2023, 1, 1, 10, 0, 0)
    end_date = datetime.datetime(2023, 1, 1, 14, 0, 0)
    
    blobs = scheme.get_blobs_in_partition(
        blob_list_getter=mock_blob_list_getter,
        prefix="bucket/dataset",
        start_date=start_date,
        end_date=end_date
    )
    
    expected_hours = [
        (2023, 1, 1, 10),
        (2023, 1, 1, 11),
        (2023, 1, 1, 12),
        (2023, 1, 1, 13),
        (2023, 1, 1, 14),
    ]
    _check_results(blobs, expected_hours)

def test_mabel_partition_scheme_multi_day():
    scheme = MabelPartitionScheme()
    
    # Test case: Range spanning 3 days
    start_date = datetime.datetime(2023, 1, 1, 22, 0, 0)
    end_date = datetime.datetime(2023, 1, 3, 2, 0, 0)
    
    blobs = scheme.get_blobs_in_partition(
        blob_list_getter=mock_blob_list_getter,
        prefix="bucket/dataset",
        start_date=start_date,
        end_date=end_date
    )
    
    expected_hours = []
    # Day 1: 22, 23
    expected_hours.extend([(2023, 1, 1, h) for h in range(22, 24)])
    # Day 2: 0-23
    expected_hours.extend([(2023, 1, 2, h) for h in range(24)])
    # Day 3: 0, 1, 2
    expected_hours.extend([(2023, 1, 3, h) for h in range(3)])
    
    _check_results(blobs, expected_hours)

def test_mabel_partition_scheme_midnight_start():
    scheme = MabelPartitionScheme()
    
    # Test case: Start exactly at midnight
    start_date = datetime.datetime(2023, 1, 2, 0, 0, 0)
    end_date = datetime.datetime(2023, 1, 2, 2, 0, 0)
    
    blobs = scheme.get_blobs_in_partition(
        blob_list_getter=mock_blob_list_getter,
        prefix="bucket/dataset",
        start_date=start_date,
        end_date=end_date
    )
    
    expected_hours = [
        (2023, 1, 2, 0),
        (2023, 1, 2, 1),
        (2023, 1, 2, 2),
    ]
    _check_results(blobs, expected_hours)

def test_mabel_partition_scheme_midnight_end():
    scheme = MabelPartitionScheme()
    
    # Test case: End exactly at midnight (start of next day)
    # Note: end_date is inclusive in date_range logic usually, but let's see how it behaves.
    # If end_date is 2023-01-02 00:00:00, it should include hour 0 of day 2.
    start_date = datetime.datetime(2023, 1, 1, 22, 0, 0)
    end_date = datetime.datetime(2023, 1, 2, 0, 0, 0)
    
    blobs = scheme.get_blobs_in_partition(
        blob_list_getter=mock_blob_list_getter,
        prefix="bucket/dataset",
        start_date=start_date,
        end_date=end_date
    )
    
    expected_hours = [
        (2023, 1, 1, 22),
        (2023, 1, 1, 23),
        (2023, 1, 2, 0),
    ]
    _check_results(blobs, expected_hours)

if __name__ == "__main__":
    test_mabel_partition_scheme_hourly_cycling()
    test_mabel_partition_scheme_same_day()
    test_mabel_partition_scheme_multi_day()
    test_mabel_partition_scheme_midnight_start()
    test_mabel_partition_scheme_midnight_end()
    print("All tests passed!")
