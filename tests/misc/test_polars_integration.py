import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../.."))


def test_polars():
    import polars

    import opteryx
    from opteryx.exceptions import NotSupportedError

    polars_df = polars.read_csv("testdata/flat/formats/csv/tweets.csv")

    try:
        opteryx.register_df("twitter", polars_df)
    except NotSupportedError:
        # skip this test
        return True
    
    curr = opteryx.Connection().cursor()
    # this is the same statement as the CSV format test
    curr.execute("SELECT username, user_verified FROM twitter WHERE username ILIKE '%cve%'")
    assert curr.shape == (2532, 2)

    # execute a join across a dataframe an another dataset
    curr = opteryx.Connection().cursor()
    curr.execute(
        "SELECT username, user_verified, name FROM twitter INNER JOIN $planets ON twitter.followers = $planets.id"
    )
    assert curr.shape == (402, 3), curr.shape


if __name__ == "__main__":  # pragma: no cover
    from tests.tools import run_tests

    run_tests()
