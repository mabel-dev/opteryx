from typing import Iterable, Dict, Any


def html_table(dictset: Iterable[dict], limit: int = 5):
    """
    Render the dictset as a HTML table.

    NOTE:
        This exhausts generators so is only recommended to be used on lists.

    Parameters:
        dictset: iterable of dictionaries
            The dictset to render
        limit: integer (optional)
            The maximum number of record to show in the table, defaults to 5

    Returns:
        string (HTML table)
    """

    def sanitize(htmlstring):
        ## some types need converting to a string first
        if isinstance(htmlstring, (list, tuple, set)) or hasattr(htmlstring, "as_list"):
            return "[ " + ", ".join([sanitize(i) for i in htmlstring]) + " ]"
        if hasattr(htmlstring, "items"):
            return sanitize(
                "{ " + ", ".join([f'"{k}": {v}' for k, v in htmlstring.items()]) + " }"
            )
        if not isinstance(htmlstring, str):
            return htmlstring
        escapes = {'"': "&quot;", "'": "&#39;", "<": "&lt;", ">": "&gt;", "$": "&#x24;"}
        # This is done first to prevent escaping other escapes.
        htmlstring = htmlstring.replace("&", "&amp;")
        for seq, esc in escapes.items():
            htmlstring = htmlstring.replace(seq, esc)
        return htmlstring

    def _to_html_table(data, columns):

        yield '<table class="table table-sm">'
        for counter, record in enumerate(data):
            if counter == 0:
                yield '<thead class="thead-light"><tr>'
                for column in columns:
                    yield f"<th>{sanitize(column)}<th>\n"
                yield "</tr></thead><tbody>"

            yield "<tr>"
            for column in columns:
                sanitized = sanitize(record.get(column, ""))
                yield f"<td title='{sanitized}' style='max-width:320px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;'>{sanitized}<td>\n"
            yield "</tr>"

        yield "</tbody></table>"

    rows = []
    columns = []  # type:ignore
    i = -1
    for i, row in enumerate(iter(dictset)):
        rows.append(row)
        columns = columns + list(row.keys())
        if (i + 1) == limit:
            break
    columns = set(columns)  # type:ignore

    import types

    footer = ""
    if isinstance(dictset, types.GeneratorType):
        footer = f"\n<p>top {i+1} rows x {len(columns)} columns</p>"
        footer += "\nNOTE: the displayed records may have been spent"
    elif hasattr(dictset, "__len__"):
        footer = f"\n<p>{len(dictset)} rows x {len(columns)} columns</p>"  # type:ignore

    return "".join(_to_html_table(rows, columns)) + footer


def ascii_table(dictset: Iterable[Dict[Any, Any]], limit: int = 5):
    """
    Render the dictset as a ASCII table.

    NOTE:
        This exhausts generators so is only recommended to be used on lists.

    Parameters:
        dictset: iterable of dictionaries
            The dictset to render
        limit: integer (optional)
            The maximum number of record to show in the table, defaults to 5

    Returns:
        string (ASCII table)
    """

    def format_value(val):
        if isinstance(val, (list, tuple, set)) or hasattr(val, "as_list"):
            return "[ " + ", ".join([format_value(i) for i in val]) + " ]"
        if hasattr(val, "items"):
            return format_value(
                "{ " + ", ".join([f'"{k}": {v}' for k, v in val.items()]) + " }"
            )
        return str(val)

    result = []
    columns: dict = {}
    cache = []

    # inspect values
    for count, row in enumerate(dictset):
        if count == limit:
            break

        cache.append(row)
        for k, v in row.items():
            v = format_value(v)
            length = max(len(str(v)), len(str(k)))
            if length > columns.get(k, 0):
                columns[k] = length

    # draw table
    bars = []
    for header, width in columns.items():
        bars.append("─" * (width + 2))

    # display headers
    result.append("┌" + "┬".join(bars) + "┐")
    result.append(
        "│" + "│".join([str(k).center(v + 2) for k, v in columns.items()]) + "│"
    )
    result.append("├" + "┼".join(bars) + "┤")

    # display values
    for row in cache:
        result.append(
            "│"
            + "│".join(
                [str(format_value(v)).center(columns[k] + 2) for k, v in row.items()]
            )
            + "│"
        )

    # display footer
    result.append("└" + "┴".join(bars) + "┘")

    return "\n".join(result)
