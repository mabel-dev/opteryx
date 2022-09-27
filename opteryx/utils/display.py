# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from typing import Any, Dict, Iterable, Union


def html_table(dictset: Iterable[dict], limit: int = 5):  # pragma: no cover
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
            return str(htmlstring)
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
    elif hasattr(dictset, "__len__"):
        footer = f"\n<p>{len(dictset)} rows x {len(columns)} columns</p>"  # type:ignore

    return "".join(_to_html_table(rows, columns)) + footer


def ascii_table(
    dictset: Iterable[Dict[Any, Any]],
    limit: int = 5,
    display_width: Union[bool, int] = True,
):  # pragma: no cover
    """
    Render the dictset as a ASCII table.

    NOTE:
        This exhausts generators so is only recommended to be used on lists.

    Parameters:
        dictset: iterable of dictionaries
            The dictset to render
        limit: integer (optional)
            The maximum number of record to show in the table, defaults to 5
        display_width: integer/boolean (optional)
            The maximum width of the table, if an integer, the number of characters,
            if a boolean, True uses the display width, False disables (5000)

    Returns:
        string (ASCII table)
    """
    if isinstance(display_width, bool):
        if not display_width:
            display_width = 5000
        else:
            import shutil

            display_width = shutil.get_terminal_size((80, 20))[0]

    def format_value(val):
        if isinstance(val, (list, tuple, set)) or hasattr(val, "as_list"):
            return "[ " + ", ".join([format_value(i) for i in val]) + " ]"
        if hasattr(val, "items"):
            return format_value(
                "{ " + ", ".join([f'"{k}": {v}' for k, v in val.items()]) + " }"
            )
        return val

    result = []
    columns: dict = {}
    cache = []
    cropped = "│"

    # inspect values
    for count, row in enumerate(dictset):
        if count == limit:
            break

        cache.append(row)
        for k, value in row.items():
            value = format_value(value)
            length = max(len(str(value)), len(str(k)))
            if length > columns.get(k, 0):
                columns[k] = length

    # draw table
    bars = []
    total_width = 2
    for k, width in columns.items():
        total_width += width + 3
        if total_width < display_width:
            bars.append("─" * (width + 2))
        else:
            columns[k] = -1
            cropped = " >"

    def just(val, width):
        if isinstance(val, (int, float)):
            return " " + str(val).rjust(width) + " "
        if isinstance(val, (bool)) or val is None:
            return " " + str(val).center(width) + " "
        return " " + str(val).ljust(width) + " "

    # display headers
    result.append("┌" + "┬".join(bars) + "┐")
    result.append(
        "│"
        + "│".join([str(k).center(v + 2) for k, v in columns.items() if v > 0])
        + cropped
    )
    result.append("├" + "┼".join(bars) + "┤")

    # display values
    for row in cache:
        result.append(
            "│"
            + "│".join(
                [
                    just(format_value(v), columns[k])
                    for k, v in row.items()
                    if columns[k] > 0
                ]
            )
            + cropped
        )

    # display footer
    result.append("└" + "┴".join(bars) + "┘")

    return "\n".join(result)
