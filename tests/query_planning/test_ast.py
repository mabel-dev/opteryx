"""
The best way to test a SQL engine is to throw queries at it.

We use a library to convert to an AST, we're going to regression test this library
here.

We're going to do that by throwing queries at it.
"""
import pytest
import sqloxide


@pytest.mark.parametrize(
    "statement, expect",
    # fmt:off
    [
        # ANALYZE - there's not a lot of variation here
        ("ANALYZE TABLE dataset", [{'Analyze': {'table_name': [{'value': 'dataset', 'quote_style': None}], 'partitions': None, 'for_columns': False, 'columns': [], 'cache_metadata': False, 'noscan': False, 'compute_statistics': False}}]),
        ("ANALYZE TABLE dataset;", [{'Analyze': {'table_name': [{'value': 'dataset', 'quote_style': None}], 'partitions': None, 'for_columns': False, 'columns': [], 'cache_metadata': False, 'noscan': False, 'compute_statistics': False}}]),
        ("analyze table dataset", [{'Analyze': {'table_name': [{'value': 'dataset', 'quote_style': None}], 'partitions': None, 'for_columns': False, 'columns': [], 'cache_metadata': False, 'noscan': False, 'compute_statistics': False}}]),
        ("analyze\ntable\ndataset", [{'Analyze': {'table_name': [{'value': 'dataset', 'quote_style': None}], 'partitions': None, 'for_columns': False, 'columns': [], 'cache_metadata': False, 'noscan': False, 'compute_statistics': False}}]),

        # CREATE INDEX - again, pretty much no variation
        ("CREATE INDEX index_name ON dataset.name (name)", [{'CreateIndex': {'name': [{'value': 'index_name', 'quote_style': None}], 'table_name': [{'value': 'dataset', 'quote_style': None}, {'value': 'name', 'quote_style': None}], 'columns': [{'expr': {'Identifier': {'value': 'name', 'quote_style': None}}, 'asc': None, 'nulls_first': None}], 'unique': False, 'if_not_exists': False}}]),
        ("create index index_name on dataset.name (name)", [{'CreateIndex': {'name': [{'value': 'index_name', 'quote_style': None}], 'table_name': [{'value': 'dataset', 'quote_style': None}, {'value': 'name', 'quote_style': None}], 'columns': [{'expr': {'Identifier': {'value': 'name', 'quote_style': None}}, 'asc': None, 'nulls_first': None}], 'unique': False, 'if_not_exists': False}}]),
        ("CREATE INDEX index_name ON dataset.name (name);", [{'CreateIndex': {'name': [{'value': 'index_name', 'quote_style': None}], 'table_name': [{'value': 'dataset', 'quote_style': None}, {'value': 'name', 'quote_style': None}], 'columns': [{'expr': {'Identifier': {'value': 'name', 'quote_style': None}}, 'asc': None, 'nulls_first': None}], 'unique': False, 'if_not_exists': False}}]),
        ("CREATE INDEX index_name ON dataset.name\n\t(name);", [{'CreateIndex': {'name': [{'value': 'index_name', 'quote_style': None}], 'table_name': [{'value': 'dataset', 'quote_style': None}, {'value': 'name', 'quote_style': None}], 'columns': [{'expr': {'Identifier': {'value': 'name', 'quote_style': None}}, 'asc': None, 'nulls_first': None}], 'unique': False, 'if_not_exists': False}}]),

        # EXPLAIN
        ("EXPLAIN SELECT * FROM dataset.name;", [{'Explain': {'describe_alias': False, 'analyze': False, 'verbose': False, 'statement': {'Query': {'with': None, 'body': {'Select': {'distinct': False, 'top': None, 'projection': ['Wildcard'], 'from': [{'relation': {'Table': {'name': [{'value': 'dataset', 'quote_style': None}, {'value': 'name', 'quote_style': None}], 'alias': None, 'args': [], 'with_hints': []}}, 'joins': []}], 'lateral_views': [], 'selection': None, 'group_by': [], 'cluster_by': [], 'distribute_by': [], 'sort_by': [], 'having': None}}, 'order_by': [], 'limit': None, 'offset': None, 'fetch': None}}}}]),

        # SELECT
        ("SELECT * FROM dataset.name;", [{'Query': {'with': None, 'body': {'Select': {'distinct': False, 'top': None, 'projection': ['Wildcard'], 'from': [{'relation': {'Table': {'name': [{'value': 'dataset', 'quote_style': None}, {'value': 'name', 'quote_style': None}], 'alias': None, 'args': [], 'with_hints': []}}, 'joins': []}], 'lateral_views': [], 'selection': None, 'group_by': [], 'cluster_by': [], 'distribute_by': [], 'sort_by': [], 'having': None}}, 'order_by': [], 'limit': None, 'offset': None, 'fetch': None}}]),
    ],
    # fmt:on
)
def test_ast_builder(statement, expect):
    """
    Test an assortment of statements
    """
    ast = sqloxide.parse_sql(statement, dialect='ansi')

    assert (
        ast == expect
    ), f'AST interpreted ""{statement}"" as ""{str(ast)}""'


if __name__ == "__main__":
    print(sqloxide.parse_sql("EXPLAIN SELECT * FROM dataset.name;", dialect='ansi'))