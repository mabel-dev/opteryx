# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.


from typing import Callable

from opteryx.managers.expression import format_expression
from opteryx.planner.logical_planner import LogicalPlanNode
from opteryx.planner.logical_planner import LogicalPlanStepType

_render_registry: dict[LogicalPlanStepType, Callable[["LogicalPlanNode"], str]] = {}


def register_render(step_type: LogicalPlanStepType):
    """
    Decorator to register a rendering function for a given LogicalPlanStepType
    """

    def wrapper(func: Callable[["LogicalPlanNode"], str]):
        _render_registry[step_type] = func
        return func

    return wrapper


@register_render(LogicalPlanStepType.Filter)
def render_filter(node: LogicalPlanNode) -> str:
    return f"FILTER ({format_expression(node.condition)})"


@register_render(LogicalPlanStepType.Aggregate)
def render_aggregate(node: LogicalPlanNode) -> str:
    return f"AGGREGATE [{', '.join(format_expression(col) for col in node.aggregates)}]"


@register_render(LogicalPlanStepType.AggregateAndGroup)
def render_aggregate_group(node: LogicalPlanNode) -> str:
    aggregates = ", ".join(format_expression(col) for col in node.aggregates)
    groups = ", ".join(format_expression(col) for col in node.groups)
    return f"AGGREGATE [{aggregates}] GROUP BY [{groups}]"


@register_render(LogicalPlanStepType.Distinct)
def render_distinct(node: LogicalPlanNode) -> str:
    if node.on:
        cols = ",".join(format_expression(col) for col in node.on)
        return f"DISTINCT ON [{cols}]"
    return "DISTINCT"


@register_render(LogicalPlanStepType.Project)
def render_project(node: LogicalPlanNode) -> str:
    cols = ", ".join(format_expression(col) for col in node.columns)
    order_by = (
        f" + ({', '.join(format_expression(col) for col in node.order_by_columns)})"
        if node.order_by_columns
        else ""
    )
    except_cols = (
        f" EXCEPT ({', '.join(format_expression(col) for col in node.except_columns)})"
        if node.except_columns
        else ""
    )
    return f"PROJECT [{cols}]{except_cols}{order_by}"


@register_render(LogicalPlanStepType.Union)
def render_union(node: LogicalPlanNode) -> str:
    modifier = f" {node.modifier.upper()}" if node.modifier else ""
    columns = " [" + ", ".join(c.current_name for c in node.columns) + "]" if node.columns else ""
    return f"UNION{modifier}{columns}"


@register_render(LogicalPlanStepType.Explain)
def render_explain(node: LogicalPlanNode) -> str:
    fmt = f" (FORMAT {node.format})" if node.format else ""
    return f"EXPLAIN{' ANALYZE' if node.analyze else ''}{fmt}"


@register_render(LogicalPlanStepType.Difference)
def render_difference(_: LogicalPlanNode) -> str:
    return "DIFFERENCE"


@register_render(LogicalPlanStepType.Join)
def render_join(node: LogicalPlanNode) -> str:
    join_type = node.type.upper()
    if node.on:
        return f"{join_type} JOIN ({format_expression(node.on, True)})"
    if node.using:
        using = ",".join(map(format_expression, node.using))
        return f"{join_type} JOIN (USING {using})"
    return f"{join_type} JOIN"


@register_render(LogicalPlanStepType.Unnest)
def render_unnest(node: LogicalPlanNode) -> str:
    distinct = "DISTINCT " if node.distinct else ""
    filters = f" FILTER ({', '.join(node.filters)})" if node.filters else ""
    return f"CROSS JOIN UNNEST ({distinct}{node.unnest_column.current_name}) AS {node.unnest_alias}{filters}"


@register_render(LogicalPlanStepType.AggregateAndGroup)
def render_aggregate_and_group(node: LogicalPlanNode) -> str:
    return f"AGGREGATE [{', '.join(format_expression(col) for col in node.aggregates)}] GROUP BY [{', '.join(format_expression(col) for col in node.groups)}]"


@register_render(LogicalPlanStepType.FunctionDataset)
def render_function_dataset(node: LogicalPlanNode) -> str:
    alias = f" AS {node.alias}" if node.alias else ""
    if node.function == "FAKE":
        return f"FAKE ({', '.join(format_expression(arg) for arg in node.args)}{alias})"
    if node.function == "GENERATE_SERIES":
        return f"GENERATE SERIES ({', '.join(format_expression(arg) for arg in node.args)}){alias}"
    if node.function == "VALUES":
        return f"VALUES (({', '.join(c.value for c in node.columns)}) x {len(node.values)} AS {node.alias})"
    if node.function == "UNNEST":
        return f"UNNEST ({', '.join(format_expression(arg) for arg in node.args)}{alias})"
    if node.function == "HTTP":
        return f"HTTP ({node.url}) AS {node.alias}"
    return node.function


@register_render(LogicalPlanStepType.HeapSort)
def render_heapsort(node: LogicalPlanNode) -> str:
    order = ", ".join(
        format_expression(expr) + (" DESC" if direction == "descending" else "")
        for expr, direction in node.order_by
    )
    return f"HEAP SORT (LIMIT {node.limit}, ORDER BY [{order}])"


@register_render(LogicalPlanStepType.Limit)
def render_limit(node: LogicalPlanNode) -> str:
    limit_str = f"LIMIT ({node.limit})" if node.limit is not None else ""
    offset_str = f" OFFSET ({node.offset})" if node.offset is not None else ""
    return (limit_str + offset_str).strip()


@register_render(LogicalPlanStepType.Order)
def render_order(node: LogicalPlanNode) -> str:
    order = ", ".join(
        format_expression(expr) + (" DESC" if direction == "descending" else "")
        for expr, direction in node.order_by
    )
    return f"ORDER BY [{order}]"


@register_render(LogicalPlanStepType.Scan)
def render_scan(node: LogicalPlanNode) -> str:
    io_async = "ASYNC " if hasattr(node.connector, "async_read_blob") else ""
    date_range = ""
    if node.start_date == node.end_date and node.start_date is not None:
        date_range = f" FOR '{node.start_date}'"
    elif node.start_date is not None:
        date_range = f" FOR '{node.start_date}' TO '{node.end_date}'"
    alias = f" AS {node.alias}" if node.relation != node.alias else ""
    columns = " [" + ", ".join(c.source_column for c in node.columns) + "]" if node.columns else ""
    predicates = (
        " (" + " AND ".join(map(format_expression, node.predicates)) + ")"
        if node.predicates
        else ""
    )
    hints = f" WITH({','.join(node.hints)})" if node.hints else ""
    limit = f" LIMIT {node.limit}" if node.limit else ""
    return f"{io_async}READ ({node.relation}{alias}{date_range}{hints}){columns}{predicates}{limit}"


@register_render(LogicalPlanStepType.Set)
def render_set(node: LogicalPlanNode) -> str:
    return f"SET ({node.variable} TO {node.value.value})"


@register_render(LogicalPlanStepType.Show)
def render_show(node: LogicalPlanNode) -> str:
    if node.object_type == "VARIABLE":
        return f"SHOW ({' '.join(node.items)})"
    if node.object_type == "VIEW":
        return f"SHOW (CREATE VIEW {node.object_name})"
    return "SHOW"


@register_render(LogicalPlanStepType.ShowColumns)
def render_show_columns(node: LogicalPlanNode) -> str:
    full = " FULL" if node.full else ""
    extended = " EXTENDED" if node.extended else ""
    return f"SHOW{full}{extended} COLUMNS ({node.relation})"


@register_render(LogicalPlanStepType.Subquery)
def render_subquery(node: LogicalPlanNode) -> str:
    return f"SUBQUERY{' AS ' + node.alias if node.alias else ''}"


@register_render(LogicalPlanStepType.CTE)
def render_cte(_: LogicalPlanNode) -> str:
    return "CTE"


@register_render(LogicalPlanStepType.MetadataWriter)
def render_metadata_writer(_: LogicalPlanNode) -> str:
    return "WRITE METADATA"


@register_render(LogicalPlanStepType.Exit)
def render_exit(_: LogicalPlanNode) -> str:
    return "EXIT"
