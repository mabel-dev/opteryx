import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], ".."))
from mabel.data import STORAGE_CLASS
from mabel.adapters.disk import DiskReader
from opteryx import SqlReader
from rich import traceback

traceback.install()


def test_where():

    s = SqlReader(
        "SELECT * FROM tests.data.tweets WHERE username == 'BBCNews'",
        inner_reader=DiskReader,
        partitioning=[],
        persistence=STORAGE_CLASS.MEMORY,
    )
    assert s.count() == 6, s.count()


def test_sql_returned_rows():
    """ """
    # fmt:off
    SQL_TESTS = [
        {"statement":"SELECT * FROM tests.data.index.is", "result":65499},
        {"statement":"SELECT * FROM tests.data.index.is  WHERE user_name = 'Verizon Support'", "result":2},
        {"statement":"SELECT * FROM tests.data.index.is  WHERE user_name == 'Verizon Support'", "result":2},
        {"statement":"SELECT * FROM tests.data.index.is  WHERE `user_name` = 'Verizon Support'", "result":2},
        {"statement":"SELECT * FROM tests.data.index.is  WHERE user_name = \"Verizon Support\"", "result":2},
        {"statement":"select * from tests.data.index.is  where user_name = 'Verizon Support'", "result":2},
        {"statement":"SELECT * FROM tests.data.index.not WHERE user_name = 'Verizon Support'", "result":2},
        {"statement":"SELECT * FROM tests.data.index.is  WHERE user_name = '********'", "result":0},
        {"statement":"SELECT * FROM tests.data.index.is  WHERE user_name LIKE '_erizon _upport'", "result":2},
        {"statement":"SELECT * FROM tests.data.index.is  WHERE user_name LIKE '%Support%'", "result":31},
        {"statement":"SELECT * FROM tests.data.index.is  WHERE user_name = 'Verizon Support'", "result":2},
        {"statement":"SELECT * FROM tests.data.index.is  WHERE tweet_id = 1346604539923853313", "result":1}, 
        {"statement":"SELECT * FROM tests.data.index.is  WHERE tweet_id = 1346604539923853313 AND user_id = 4832862820", "result":1},
        {"statement":"SELECT * FROM tests.data.index.is  WHERE tweet_id IN (1346604539923853313, 1346604544134885378)", "result":2},
        {"statement":"SELECT * FROM tests.data.index.is  WHERE tweet_id = 1346604539923853313 OR user_id = 2147860407", "result":2},
        {"statement":"SELECT * FROM tests.data.index.is  WHERE tweet_id = 1346604539923853313 OR user_verified = True", "result":453},
        {"statement":"SELECT * FROM tests.data.index.is  WHERE user_name = 'Dave Jamieson' AND user_verified = True", "result":1},
        {"statement":"SELECT COUNT(*) FROM tests.data.index.is  WHERE user_name = 'Dave Jamieson' AND user_verified = True", "result":1},
        {"statement":"SELECT count(*) FROM tests.data.index.is GROUP BY user_verified", "result":2},
        {"statement":"SELECT COUNT (*) FROM tests.data.index.is GROUP BY user_verified", "result":2},
        {"statement":"SELECT Count(*) FROM tests.data.index.is GROUP BY user_verified", "result":2},
        {"statement":"SELECT COUNT(*), user_verified FROM tests.data.index.is GROUP BY user_verified", "result":2},
        {"statement":"SELECT * FROM tests.data.index.is WHERE hash_tags contains 'Georgia'", "result":50},
        {"statement":"SELECT COUNT(*) FROM (SELECT user_name FROM tests.data.index.is GROUP BY user_name)", "result":1},
        {"statement":"SELECT MAX(user_name) FROM tests.data.index.is", "result":1},
        {"statement":"SELECT AVG(followers) FROM tests.data.index.is", "result":1},
        {"statement":"SELECT * FROM tests.data.index.is ORDER BY user_name", "result":10000}, # ORDER BY is 10000 record limited
        {"statement":"SELECT * FROM tests.data.index.is ORDER BY user_name ASC", "result":10000},  # ORDER BY is 10000 record limited
        {"statement":"SELECT * FROM tests.data.index.is ORDER BY user_name DESC", "result":10000},  # ORDER BY is 10000 record limited
        {"statement":"SELECT COUNT(user_id) FROM tests.data.index.is", "result":1},
        {"statement":"SELECT * FROM tests.data.index.is WHERE user_id > 1000000", "result":65475},
        {"statement":"SELECT * FROM tests.data.index.is WHERE followers > 100.0", "result":49601},
        {"statement":"SELECT COUNT(*), user_verified, user_id FROM tests.data.index.is GROUP BY user_verified, user_id", "result":60724},
        {"statement":"SELECT * FROM tests.data.index.is WHERE user_name IN ('Steve Strong', 'noel')", "result":3},
    ]
    # fmt:on

    for test in SQL_TESTS:
        try:
            s = SqlReader(
                test.get("statement"),
                inner_reader=DiskReader,
                partitioning=[],
                persistence=STORAGE_CLASS.MEMORY,
            )
            len_s = len(s.collect_list())
            assert len_s == test.get(
                "result"
            ), f"{test.get('statement')} == {len_s} ({test.get('result')})"
        except Exception as e:
            print(test.get("statement"))
            raise e


def test_sql_to_dictset():

    s = SqlReader(
        sql_statement="SELECT * FROM tests.data.index.not",
        inner_reader=DiskReader,
        partitioning=[],
        persistence=STORAGE_CLASS.MEMORY,
    )
    keys = s.keys()
    assert "tweet_id" in keys, keys
    assert "text" in keys, keys
    assert "followers" in keys, keys
    assert len(s.take(10).collect_list()) == 10


def test_sql_returned_cols():

    # fmt:off
    SQL_TESTS = [
        {"statement":"SELECT tweet_id, user_name FROM tests.data.index.not LIMIT 1", "keys":["tweet_id","user_name"]},
        {"statement":"SELECT COUNT(*) AS count FROM tests.data.index.not LIMIT 1", "keys": ["count"]},
        {"statement":"SELECT COUNT(*) FROM tests.data.index.not LIMIT 1", "keys": ["COUNT(*)"]}
                
    ]
    # fmt:on

    for test in SQL_TESTS:
        s = SqlReader(
            test.get("statement"),
            inner_reader=DiskReader,
            partitioning=[],
            persistence=STORAGE_CLASS.MEMORY,
        )
        # print(s.collect())
        cols = s.keys()
        assert sorted(cols) == sorted(test["keys"]), f"{test.get('statement')} - {cols}"


def test_limit():

    s = SqlReader(
        sql_statement="SELECT tweet_id, user_name FROM tests.data.index.not LIMIT 12",
        inner_reader=DiskReader,
        partitioning=[],
        persistence=STORAGE_CLASS.MEMORY,
    )
    record_count = s.count()
    assert record_count == 12, record_count


def test_group_by_count():

    s = SqlReader(
        sql_statement="SELECT COUNT(*) FROM tests.data.index.not GROUP BY user_verified",
        inner_reader=DiskReader,
        partitioning=[],
    )
    records = s.collect_list()
    record_count = len(records)
    assert record_count == 2, record_count

    s = SqlReader(
        sql_statement="SELECT COUNT(*), user_name FROM tests.data.index.not GROUP BY user_name",
        inner_reader=DiskReader,
        partitioning=[],
    )
    records = s.collect_list()
    record_count = len(records)
    assert record_count == 56527, record_count


if __name__ == "__main__":  # pragma: no cover
    import mabel

    print(mabel.__version__)

    test_sql_returned_rows()
    test_sql_to_dictset()
    test_sql_returned_cols()
    test_where()
    test_limit()
    test_group_by_count()

    print("okay")
