import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

from opteryx.utils.file_decoders import parquet_decoder
from opteryx.compiled.structures.relation_statistics import to_int

def test_read_statistics_tweets():
    with open("testdata/flat/formats/parquet/tweets.parquet", "rb") as f:
        data = f.read()

        stats = parquet_decoder(data, just_statistics=True)

    assert stats.record_count == 100000, stats.record_count

    assert stats.lower_bounds[b"tweet_id"] == to_int(1346604539013705728)
    assert stats.upper_bounds[b"tweet_id"] == to_int(1346615999009755142)
    assert stats.null_count.get(b"tweet_id", 0) == 0

    assert stats.lower_bounds[b"is_quoting"] == to_int(28466111963996160)
    assert stats.upper_bounds[b"is_quoting"] == to_int(1346615755694104578)
    assert stats.null_count[b"is_quoting"] == 85598


def test_read_statistics_planets():
    with open("testdata/planets/planets.parquet", "rb") as f:
        data = f.read()

        stats = parquet_decoder(data, just_statistics=True)

    assert stats.record_count == 9

    assert stats.lower_bounds[b"id"] == 1
    assert stats.upper_bounds[b"id"] == 9
    assert stats.null_count.get(b"id", 0) == 0
    
    assert stats.lower_bounds[b"name"] == to_int("Earth")
    assert stats.upper_bounds[b"name"] == to_int("Venus")
    assert stats.null_count.get(b"name", 0) == 0

    assert stats.lower_bounds[b"surfacePressure"] == to_int(0.0)
    assert stats.upper_bounds[b"surfacePressure"] == to_int(92.0)



if __name__ == "__main__":  # pragma: no cover
    from tests import run_tests

    test_read_statistics_planets()

    run_tests()
