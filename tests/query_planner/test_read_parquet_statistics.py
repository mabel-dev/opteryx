import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

from opteryx.utils.file_decoders import parquet_decoder

def test_read_statistics_tweets():
    with open("testdata/flat/formats/parquet/tweets.parquet", "rb") as f:
        data = f.read()

        stats = parquet_decoder(data, just_statistics=True)

    assert stats.record_count == 100000, stats.record_count

    assert stats.lower_bounds["tweet_id"] == 1346604539013705728
    assert stats.upper_bounds["tweet_id"] == 1346615999009755142
    assert stats.null_count["tweet_id"] == 0

    assert stats.lower_bounds["is_quoting"] == 28466111963996160
    assert stats.upper_bounds["is_quoting"] == 1346615755694104578, stats.upper_bounds["is_quoting"]
    assert stats.null_count["is_quoting"] == 85598


def test_read_statistics_planets():
    with open("testdata/planets/planets.parquet", "rb") as f:
        data = f.read()

        stats = parquet_decoder(data, just_statistics=True)

    assert stats.record_count == 9

    assert stats.lower_bounds["id"] == 1
    assert stats.upper_bounds["id"] == 9
    assert stats.null_count["id"] == 0
    
    assert stats.lower_bounds["name"] == "Earth"
    assert stats.upper_bounds["name"] == "Venus"
    assert stats.null_count["name"] == 0

    assert stats.lower_bounds["surfacePressure"] == 0.0
    assert stats.upper_bounds["surfacePressure"] == 92.0



def test_read_statistics():
    with open("testdata/astronauts/astronauts.parquet", "rb") as f:
        data = f.read()

        stats = parquet_decoder(data, just_statistics=True)

    print(stats.record_count)
    print(stats.lower_bounds)
    print(stats.upper_bounds)
    print(stats.null_count)


if __name__ == "__main__":  # pragma: no cover
    from tests.tools import run_tests

    run_tests()
