import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../.."))


def test_pandas():
    import opteryx
    import pandas

    pandas_df = pandas.read_csv("testdata/flat/formats/csv/tweets.csv")

    opteryx.register_df("twitter", pandas_df)
    curr = opteryx.Connection().cursor()
    # this is the same statement as the CSV format test
    curr.execute("SELECT username, user_verified FROM twitter WHERE username ILIKE '%cve%'")
    assert curr.shape == (2532, 2)

    # execute a join across a dataframe an another dataset
    curr.execute(
        "SELECT username, user_verified, name FROM twitter INNER JOIN $planets ON twitter.followers = $planets.id"
    )
    assert curr.shape == (402, 3), curr.shape


def test_documentation():
    import opteryx
    import pandas

    pandas_df = pandas.read_csv("https://storage.googleapis.com/opteryx/exoplanets/exoplanets.csv")

    opteryx.register_df("exoplanets", pandas_df)
    curr = opteryx.Connection().cursor()
    curr.execute("SELECT koi_disposition, COUNT(*) FROM exoplanets GROUP BY koi_disposition;")
    aggregrated_df = curr.to_df()

    assert len(aggregrated_df) == 3
    assert len(aggregrated_df.columns) == 2


if __name__ == "__main__":  # pragma: no cover
    test_pandas()
    test_documentation()

    print("✅ okay")
