import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../.."))


def test_polars():
    import opteryx
    import polars

    polars_df = polars.read_csv("testdata/flat/formats/csv/tweets.csv")

    opteryx.register_df("twitter", polars_df)
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
    test_polars()

    print("✅ okay")
