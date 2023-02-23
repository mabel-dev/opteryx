# isort: skip_file
from textwrap import dedent

from .builder import Query


def test_query_simple():
    query = Query().SELECT("select").FROM("from").JOIN("join").WHERE("where")
    assert str(query) == dedent(
        """\
        SELECT
            select
        FROM
            from
        JOIN
            join
        WHERE
            where
        """
    )


def test_query_complicated():
    """Test a complicated query:

    * order between different keywords does not matter
    * arguments of repeated calls get appended, with the order preserved
    * SELECT can receive 2-tuples
    * WHERE and HAVING arguments are separated by AND
    * JOIN arguments are separated by the keyword, and come after plain FROM
    * no-argument keywords have no effect, unless they are flags

    """
    query = (
        Query()
        .WHERE()
        .OUTER_JOIN("outer join")
        .JOIN("join")
        .LIMIT("limit")
        .JOIN()
        .ORDER_BY("first", "second")
        .SELECT("one")
        .HAVING("having")
        .SELECT(("two", "expr"))
        .GROUP_BY("group by")
        .FROM("from")
        .SELECT("three", "four")
        .FROM("another from")
        .WHERE("where")
        .ORDER_BY("third")
        .OUTER_JOIN("another outer join")
        # this isn't technically valid
        .WITH("first cte")
        .GROUP_BY("another group by")
        .HAVING("another having")
        .WITH(("fancy", "second cte"))
        .JOIN("another join")
        .WHERE("another where")
        .NATURAL_JOIN("natural join")
        .SELECT()
        .SELECT_DISTINCT()
    )
    assert str(query) == dedent(
        """\
        WITH
            (
                first cte
            ),
            fancy AS (
                second cte
            )
        SELECT DISTINCT
            one,
            expr AS two,
            three,
            four
        FROM
            from,
            another from
        OUTER JOIN
            outer join
        JOIN
            join
        OUTER JOIN
            another outer join
        JOIN
            another join
        NATURAL JOIN
            natural join
        WHERE
            where AND
            another where
        GROUP BY
            group by,
            another group by
        HAVING
            having AND
            another having
        ORDER BY
            first,
            second,
            third
        LIMIT
            limit
        """
    )


def test_query_init():
    query = Query({"(": ["one", "two", "three"], ")": [""]}, {"(": "OR"})
    assert str(query) == dedent(
        """\
        (
            one OR
            two OR
            three
        )

        """
    )
