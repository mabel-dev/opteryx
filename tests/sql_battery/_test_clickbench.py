import os
import pytest
import sys

os.environ["OPTERYX_DEBUG"] = ""

sys.path.insert(1, os.path.join(sys.path[0], "../../../draken"))
sys.path.insert(1, os.path.join(sys.path[0], "../../../orso"))
sys.path.insert(1, os.path.join(sys.path[0], "../../../rugo"))
sys.path.insert(1, os.path.join(sys.path[0], "../.."))

from typing import Optional

import opteryx
from opteryx.utils.formatter import format_sql

# fmt:off
STATEMENTS = [

        ("/* 01 */ SELECT COUNT(*) FROM testdata.clickbench_tiny;", None),
        ("/* 02 */ SELECT COUNT(*) FROM testdata.clickbench_tiny WHERE AdvEngineID <> 0;", None),
        ("/* 03 */ SELECT SUM(AdvEngineID), COUNT(*), AVG(ResolutionWidth) FROM testdata.clickbench_tiny;", None),
        ("/* 04 */ SELECT AVG(UserID) FROM testdata.clickbench_tiny;", None),
        ("/* 05 */ SELECT COUNT(DISTINCT UserID) FROM testdata.clickbench_tiny;", None),
        ("/* 06 */ SELECT COUNT(DISTINCT SearchPhrase) FROM testdata.clickbench_tiny;", None),
        ("/* 07 */ SELECT MIN(EventDate), MAX(EventDate) FROM testdata.clickbench_tiny;", None),
        ("/* 08 */ SELECT AdvEngineID, COUNT(*) FROM testdata.clickbench_tiny WHERE AdvEngineID <> 0 GROUP BY AdvEngineID ORDER BY COUNT(*) DESC;", None),
        ("/* 09 */ SELECT RegionID, COUNT(DISTINCT UserID) AS u FROM testdata.clickbench_tiny GROUP BY RegionID ORDER BY u DESC LIMIT 10;", None),
        ("/* 10 */ SELECT RegionID, SUM(AdvEngineID), COUNT(*) AS c, AVG(ResolutionWidth), COUNT(DISTINCT UserID) FROM testdata.clickbench_tiny GROUP BY RegionID ORDER BY c DESC LIMIT 10;", None),
        ("/* 11 */ SELECT MobilePhoneModel, COUNT(DISTINCT UserID) AS u FROM testdata.clickbench_tiny WHERE MobilePhoneModel <> '' GROUP BY MobilePhoneModel ORDER BY u DESC LIMIT 10;", None),
        ("/* 12 */ SELECT MobilePhone, MobilePhoneModel, COUNT(DISTINCT UserID) AS u FROM testdata.clickbench_tiny WHERE MobilePhoneModel <> '' GROUP BY MobilePhone, MobilePhoneModel ORDER BY u DESC LIMIT 10;", None),
        ("/* 13 */ SELECT SearchPhrase, COUNT(*) AS c FROM testdata.clickbench_tiny WHERE SearchPhrase <> '' GROUP BY SearchPhrase ORDER BY c DESC LIMIT 10;", None),
        ("/* 14 */ SELECT SearchPhrase, COUNT(DISTINCT UserID) AS u FROM testdata.clickbench_tiny WHERE SearchPhrase <> '' GROUP BY SearchPhrase ORDER BY u DESC LIMIT 10;", None),
        ("/* 15 */ SELECT SearchEngineID, SearchPhrase, COUNT(*) AS c FROM testdata.clickbench_tiny WHERE SearchPhrase <> '' GROUP BY SearchEngineID, SearchPhrase ORDER BY c DESC LIMIT 10;", None),
        ("/* 16 */ SELECT UserID, COUNT(*) FROM testdata.clickbench_tiny GROUP BY UserID ORDER BY COUNT(*) DESC LIMIT 10;", None),
        ("/* 17 */ SELECT UserID, SearchPhrase, COUNT(*) FROM testdata.clickbench_tiny GROUP BY UserID, SearchPhrase ORDER BY COUNT(*) DESC LIMIT 10;", None),
        ("/* 18 */ SELECT UserID, SearchPhrase, COUNT(*) FROM testdata.clickbench_tiny GROUP BY UserID, SearchPhrase LIMIT 10;", None),
        ("/* 19 */ SELECT UserID, extract(minute FROM EventTime) AS m, SearchPhrase, COUNT(*) FROM testdata.clickbench_tiny GROUP BY UserID, extract(minute FROM EventTime), SearchPhrase ORDER BY COUNT(*) DESC LIMIT 10;", None),
        ("/* 20 */ SELECT UserID FROM testdata.clickbench_tiny WHERE UserID = 435090932899640449;", None),
        ("/* 21 */ SELECT COUNT(*) FROM testdata.clickbench_tiny WHERE URL LIKE '%google%';", None),
        ("/* 22 */ SELECT SearchPhrase, MIN(URL), COUNT(*) AS c FROM testdata.clickbench_tiny WHERE URL LIKE '%google%' AND SearchPhrase <> '' GROUP BY SearchPhrase ORDER BY c DESC LIMIT 10;", None),
        ("/* 23 */ SELECT SearchPhrase, MIN(URL), MIN(Title), COUNT(*) AS c, COUNT(DISTINCT UserID) FROM testdata.clickbench_tiny WHERE Title LIKE '%Google%' AND URL NOT LIKE '%.google.%' AND SearchPhrase <> '' GROUP BY SearchPhrase ORDER BY c DESC LIMIT 10;", None),
        ("/* 24 */ SELECT * FROM testdata.clickbench_tiny WHERE URL LIKE '%google%' ORDER BY EventTime LIMIT 10;", None),
        ("/* 25 */ SELECT SearchPhrase FROM testdata.clickbench_tiny WHERE SearchPhrase <> '' ORDER BY EventTime LIMIT 10;", None),
        ("/* 26 */ SELECT SearchPhrase FROM testdata.clickbench_tiny WHERE SearchPhrase <> '' ORDER BY SearchPhrase LIMIT 10;", None),
        ("/* 27 */ SELECT SearchPhrase FROM testdata.clickbench_tiny WHERE SearchPhrase <> '' ORDER BY EventTime, SearchPhrase LIMIT 10;", None),
        ("/* 28 */ SELECT CounterID, AVG(length(URL)) AS l, COUNT(*) AS c FROM testdata.clickbench_tiny WHERE URL <> '' GROUP BY CounterID HAVING COUNT(*) > 100000 ORDER BY l DESC LIMIT 25;", None),
        ("/* 29 */ SELECT REGEXP_REPLACE(Referer, b'^https?://(?:www\.)?([^/]+)/.*$', r'\\1') AS k, AVG(length(Referer)) AS l, COUNT(*) AS c, MIN(Referer) FROM testdata.clickbench_tiny WHERE Referer <> '' GROUP BY REGEXP_REPLACE(Referer, b'^https?://(?:www\.)?([^/]+)/.*$', r'\\1') HAVING COUNT(*) > 100000 ORDER BY l DESC LIMIT 25;", None),
        ("/* 30 */ SELECT SUM(ResolutionWidth), SUM(ResolutionWidth + 1), SUM(ResolutionWidth + 2), SUM(ResolutionWidth + 3), SUM(ResolutionWidth + 4), SUM(ResolutionWidth + 5), SUM(ResolutionWidth + 6), SUM(ResolutionWidth + 7), SUM(ResolutionWidth + 8), SUM(ResolutionWidth + 9), SUM(ResolutionWidth + 10), SUM(ResolutionWidth + 11), SUM(ResolutionWidth + 12), SUM(ResolutionWidth + 13), SUM(ResolutionWidth + 14), SUM(ResolutionWidth + 15), SUM(ResolutionWidth + 16), SUM(ResolutionWidth + 17), SUM(ResolutionWidth + 18), SUM(ResolutionWidth + 19), SUM(ResolutionWidth + 20), SUM(ResolutionWidth + 21), SUM(ResolutionWidth + 22), SUM(ResolutionWidth + 23), SUM(ResolutionWidth + 24), SUM(ResolutionWidth + 25), SUM(ResolutionWidth + 26), SUM(ResolutionWidth + 27), SUM(ResolutionWidth + 28), SUM(ResolutionWidth + 29), SUM(ResolutionWidth + 30), SUM(ResolutionWidth + 31), SUM(ResolutionWidth + 32), SUM(ResolutionWidth + 33), SUM(ResolutionWidth + 34), SUM(ResolutionWidth + 35), SUM(ResolutionWidth + 36), SUM(ResolutionWidth + 37), SUM(ResolutionWidth + 38), SUM(ResolutionWidth + 39), SUM(ResolutionWidth + 40), SUM(ResolutionWidth + 41), SUM(ResolutionWidth + 42), SUM(ResolutionWidth + 43), SUM(ResolutionWidth + 44), SUM(ResolutionWidth + 45), SUM(ResolutionWidth + 46), SUM(ResolutionWidth + 47), SUM(ResolutionWidth + 48), SUM(ResolutionWidth + 49), SUM(ResolutionWidth + 50), SUM(ResolutionWidth + 51), SUM(ResolutionWidth + 52), SUM(ResolutionWidth + 53), SUM(ResolutionWidth + 54), SUM(ResolutionWidth + 55), SUM(ResolutionWidth + 56), SUM(ResolutionWidth + 57), SUM(ResolutionWidth + 58), SUM(ResolutionWidth + 59), SUM(ResolutionWidth + 60), SUM(ResolutionWidth + 61), SUM(ResolutionWidth + 62), SUM(ResolutionWidth + 63), SUM(ResolutionWidth + 64), SUM(ResolutionWidth + 65), SUM(ResolutionWidth + 66), SUM(ResolutionWidth + 67), SUM(ResolutionWidth + 68), SUM(ResolutionWidth + 69), SUM(ResolutionWidth + 70), SUM(ResolutionWidth + 71), SUM(ResolutionWidth + 72), SUM(ResolutionWidth + 73), SUM(ResolutionWidth + 74), SUM(ResolutionWidth + 75), SUM(ResolutionWidth + 76), SUM(ResolutionWidth + 77), SUM(ResolutionWidth + 78), SUM(ResolutionWidth + 79), SUM(ResolutionWidth + 80), SUM(ResolutionWidth + 81), SUM(ResolutionWidth + 82), SUM(ResolutionWidth + 83), SUM(ResolutionWidth + 84), SUM(ResolutionWidth + 85), SUM(ResolutionWidth + 86), SUM(ResolutionWidth + 87), SUM(ResolutionWidth + 88), SUM(ResolutionWidth + 89) FROM testdata.clickbench_tiny;", None),
        ("/* 31 */ SELECT SearchEngineID, ClientIP, COUNT(*) AS c, SUM(IsRefresh), AVG(ResolutionWidth) FROM testdata.clickbench_tiny WHERE SearchPhrase <> '' GROUP BY SearchEngineID, ClientIP ORDER BY c DESC LIMIT 10;", None),
        ("/* 32 */ SELECT WatchID, ClientIP, COUNT(*) AS c, SUM(IsRefresh), AVG(ResolutionWidth) FROM testdata.clickbench_tiny WHERE SearchPhrase <> '' GROUP BY WatchID, ClientIP ORDER BY c DESC LIMIT 10;", None),
        ("/* 33 */ SELECT WatchID, ClientIP, COUNT(*) AS c, SUM(IsRefresh), AVG(ResolutionWidth) FROM testdata.clickbench_tiny GROUP BY WatchID, ClientIP ORDER BY c DESC LIMIT 10;", None),
        ("-- /* 34 */ SELECT URL, COUNT(*) AS c FROM testdata.clickbench_tiny GROUP BY URL ORDER BY c DESC LIMIT 10;", None),
        ("-- /* 35 */ SELECT 1, URL, COUNT(*) AS c FROM testdata.clickbench_tiny GROUP BY 1, URL ORDER BY c DESC LIMIT 10;", None),
        ("/* 36 */ SELECT ClientIP, ClientIP - 1, ClientIP - 2, ClientIP - 3, COUNT(*) AS c FROM testdata.clickbench_tiny GROUP BY ClientIP, ClientIP - 1, ClientIP - 2, ClientIP - 3 ORDER BY c DESC LIMIT 10;", None),
        ("/* 37 */ SELECT URL, COUNT(*) AS PageViews FROM testdata.clickbench_tiny WHERE CounterID = 62 AND EventDate >= '2013-07-01' AND EventDate <= '2013-07-31' AND DontCountHits = 0 AND IsRefresh = 0 AND URL <> '' GROUP BY URL ORDER BY PageViews DESC LIMIT 10;", None),
        ("/* 38 */ SELECT Title, COUNT(*) AS PageViews FROM testdata.clickbench_tiny WHERE CounterID = 62 AND EventDate >= '2013-07-01' AND EventDate <= '2013-07-31' AND DontCountHits = 0 AND IsRefresh = 0 AND Title <> '' GROUP BY Title ORDER BY PageViews DESC LIMIT 10;", None),
        ("/* 39 */ SELECT URL, COUNT(*) AS PageViews FROM testdata.clickbench_tiny WHERE CounterID = 62 AND EventDate >= '2013-07-01' AND EventDate <= '2013-07-31' AND IsRefresh = 0 AND IsLink <> 0 AND IsDownload = 0 GROUP BY URL ORDER BY PageViews DESC LIMIT 10 OFFSET 1000;", None),
        ("/* 40 */ SELECT TraficSourceID, SearchEngineID, AdvEngineID, CASE WHEN (SearchEngineID = 0 AND AdvEngineID = 0) THEN Referer ELSE '' END AS Src, URL AS Dst, COUNT(*) AS PageViews FROM testdata.clickbench_tiny WHERE CounterID = 62 AND EventDate >= '2013-07-01' AND EventDate <= '2013-07-31' AND IsRefresh = 0 GROUP BY TraficSourceID, SearchEngineID, AdvEngineID, CASE WHEN (SearchEngineID = 0 AND AdvEngineID = 0) THEN Referer ELSE '' END, URL ORDER BY PageViews DESC LIMIT 10 OFFSET 1000;", None),
        ("/* 41 */ SELECT URLHash, EventDate, COUNT(*) AS PageViews FROM testdata.clickbench_tiny WHERE CounterID = 62 AND EventDate >= '2013-07-01' AND EventDate <= '2013-07-31' AND IsRefresh = 0 AND TraficSourceID IN (-1, 6) AND RefererHash = 3594120000172545465 GROUP BY URLHash, EventDate ORDER BY PageViews DESC LIMIT 10 OFFSET 100;", None),
        ("/* 42 */ SELECT WindowClientWidth, WindowClientHeight, COUNT(*) AS PageViews FROM testdata.clickbench_tiny WHERE CounterID = 62 AND EventDate >= '2013-07-01' AND EventDate <= '2013-07-31' AND IsRefresh = 0 AND DontCountHits = 0 AND URLHash = 2868770270353813622 GROUP BY WindowClientWidth, WindowClientHeight ORDER BY PageViews DESC LIMIT 10 OFFSET 10000;", None),
        ("/* 43 */ SELECT DATE_TRUNC('minute', EventTime) AS M, COUNT(*) AS PageViews FROM testdata.clickbench_tiny WHERE CounterID = 62 AND EventDate >= '2013-07-14' AND EventDate <= '2013-07-15' AND IsRefresh = 0 AND DontCountHits = 0 GROUP BY DATE_TRUNC('minute', EventTime) ORDER BY M LIMIT 10 OFFSET 1000;", None),
]
# fmt:on


@pytest.mark.parametrize("statement, exception", STATEMENTS)
def test_sql_battery(statement:str, exception: Optional[Exception]):
    """
    Test an battery of statements
    """

    try:
        # query to arrow is the fastest way to query
        result = opteryx.query_to_arrow(statement)
        result.shape
        assert (
            exception is None
        ), f"Exception {exception} not raised but expected\n{format_sql(statement)}"
    except AssertionError as error:
        raise error
    except Exception as error:
        if not type(error) == exception:
            raise ValueError(
                f"{format_sql(statement)}\nQuery failed with error {type(error)} but error {exception} was expected"
            ) from error


if __name__ == "__main__":  # pragma: no cover
    # Running in the IDE we do some formatting - it's not functional but helps when reading the outputs.

    import shutil
    import time

    from tests.tools import trunc_printable

    start_suite = time.monotonic_ns()
    width = shutil.get_terminal_size((80, 20))[0] - 18
    passed:int = 0
    failed:int = 0
    nl:str = "\n"
    failures = []

    print(f"RUNNING CLICKBENCH BATTERY OF {len(STATEMENTS)} QUERIES\n")
    for index, (statement, err) in enumerate(STATEMENTS):
        statement = statement.replace("testdata.clickbench_tiny", "hits")
        printable = statement
        print(
            f"\033[38;2;255;184;108m{(index + 1):04}\033[0m"
            f" {trunc_printable(format_sql(printable), width - 1)}",
            end="",
            flush=True,
        )
        try:
            start = time.monotonic_ns()
            test_sql_battery(statement, err)
            print(
                f"\033[38;2;26;185;67m{str(int((time.monotonic_ns() - start)/1e6)).rjust(4)}ms\033[0m ✅",
                end="",
            )
            passed += 1
            if failed > 0:
                print(f" \033[0;31m{failed}\033[0m")
            else:
                print()
        except Exception as err:
            failed += 1
            print(f"\033[0;31m{str(int((time.monotonic_ns() - start)/1e6)).rjust(4)}ms ❌ {failed}\033[0m")
            print(">", err)
            failures.append((statement, err))
            
            #print(opteryx.query(statement))
            #raise err

    print("--- ✅ \033[0;32mdone\033[0m")

    if failed > 0:
        print("\n\033[38;2;139;233;253m\033[3mFAILURES\033[0m")
        for statement, err in failures:
            print(err)

    print(
        f"\n\033[38;2;139;233;253m\033[3mCOMPLETE\033[0m ({((time.monotonic_ns() - start_suite) / 1e9):.2f} seconds)\n"
        f"  \033[38;2;26;185;67m{passed} passed ({(passed * 100) // (passed + failed)}%)\033[0m\n"
        f"  \033[38;2;255;121;198m{failed} failed\033[0m"
    )
