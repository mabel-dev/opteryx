import re
from typing import List

import numpy
import pyarrow

ESCAPE_SPECIAL_CHARS = re.compile(r"([.^$*+?{}[\]|()\\])")


def sql_like_to_regex(pattern: str, full_match: bool = True, case_sensitive: bool = True) -> str:
    """
    Converts an SQL `LIKE` pattern into a regular expression.

    SQL `LIKE` syntax:
    - `%` matches zero or more characters (similar to `.*` in regex).
    - `_` matches exactly one character (similar to `.` in regex).
    - Special regex characters are escaped to ensure literal matching.

    Args:
        pattern (str): The SQL LIKE pattern.

    Returns:
        str: The equivalent regex pattern.

    Examples:
        sql_like_to_regex("a%")       -> "^a.*?$"
        sql_like_to_regex("_b")       -> "^.b$"
        sql_like_to_regex("%[test]%") -> "^.*?\[test\].*?$"
    """
    if pattern is None:
        raise ValueError("Pattern cannot be None")

    if isinstance(pattern, bytes):
        pattern = pattern.decode("utf-8")

    # Escape special regex characters in the pattern
    escaped_pattern = ESCAPE_SPECIAL_CHARS.sub(r"\\\1", pattern)

    # Replace SQL wildcards with regex equivalents
    regex_pattern = "^" + escaped_pattern.replace("%", ".*?").replace("_", ".") + "$"
    if not full_match:
        if regex_pattern.startswith("^.*?"):
            regex_pattern = regex_pattern[4:]
        if regex_pattern.endswith(".*?$"):
            regex_pattern = regex_pattern[:-4]
    if not case_sensitive:
        regex_pattern = f"(?i)({regex_pattern})"
    return regex_pattern


def remove_comments(string: str) -> str:
    """
    Remove comments from the string.

    Parameters:
        string: str
            The SQL query string from which comments are to be removed.

    Returns:
        str: The SQL query string with comments removed.
    """
    # First group captures quoted strings (double or single)
    # Second group captures comments (/* multi-line */ or -- single-line)
    pattern = r"(\"[^\"]*\"|\'[^\']*\')|(/\*.*?\*/|--[^\r\n]*$)"

    regex = re.compile(pattern, re.MULTILINE | re.DOTALL)

    def _replacer(match):
        if match.group(2) is not None:
            return ""  # Remove the comment
        else:
            return match.group(1)  # Keep the quoted string

    return regex.sub(_replacer, string)


def clean_statement(string: str) -> str:
    """
    Remove carriage returns and all whitespace to single spaces.

    Avoid removing whitespace in quoted strings.
    """
    pattern = r"(\"[^\"]*\"|\'[^\']*\'|\`[^\`]*\`)|(\s+)"
    regex = re.compile(pattern, re.MULTILINE | re.DOTALL)

    def _replacer(match):
        if match.group(2) is not None:
            return " "
        return match.group(1)  # captured quoted-string

    return regex.sub(_replacer, string).strip()


def split_sql_statements(sql: str) -> List[str]:
    """
    Splits multiple SQL statements separated by semicolons into a list.

    Parameters:
        sql: str
            A string containing one or more SQL statements.

    Returns:
        List[str]: A list of individual SQL statements.
    """
    statements = []
    buffer: list = []
    in_single_quote = False
    in_double_quote = False
    in_backtick_quote = False

    for char in sql:
        if char == "'" and not in_double_quote and not in_backtick_quote:
            in_single_quote = not in_single_quote
        elif char == '"' and not in_single_quote and not in_backtick_quote:
            in_double_quote = not in_double_quote
        elif char == "`" and not in_single_quote and not in_double_quote:
            in_backtick_quote = not in_backtick_quote
        elif char == ";" and not in_single_quote and not in_double_quote and not in_backtick_quote:
            statements.append("".join(buffer).strip())
            buffer = []
            continue

        buffer.append(char)

    # Append any remaining text
    if buffer:
        statements.append("".join(buffer).strip())

    return [s for s in statements if s != ""]


def regex_match_any(
    arr: pyarrow.Array,
    patterns: List[str],
    flags: int = 0,
    invert: bool = False,
) -> numpy.ndarray:
    """
    Evaluates whether each row in `arr` matches ANY of the given LIKE patterns.
    Compatible with Arrow Arrays (flat or List<String>).

    Parameters:
        arr: pyarrow.Array or ChunkedArray
        patterns: list of SQL LIKE patterns (converted to regex)
        flags: regex flags (e.g. re.IGNORECASE)
        invert: True to negate the result (i.e., NOT LIKE ANY)

    Returns:
        numpy.ndarray of object dtype (bool or None per row)
    """
    patterns = patterns[0]

    if hasattr(patterns, "to_pylist"):
        patterns = patterns.to_pylist()
    if any(not isinstance(p, str) for p in patterns if p):
        from opteryx.exceptions import IncorrectTypeError

        raise IncorrectTypeError("Patterns for LIKE ANY comparisons must be strings.")

    # Compile a single combined regex
    pattern_str = r"|".join(sql_like_to_regex(p) for p in patterns if p) or r"(?!x)"
    combined_regex = re.compile(pattern_str, flags=flags)

    # Normalize to a flat list of Arrow chunks
    chunks = arr.chunks if isinstance(arr, pyarrow.ChunkedArray) else [arr]
    total_len = sum(len(chunk) for chunk in chunks)
    out = numpy.empty(total_len, dtype=object)

    offset = 0
    for chunk in chunks:
        if pyarrow.types.is_list(chunk.type):
            values = chunk.values.to_pylist()
            offsets = chunk.offsets.to_numpy()
            validity = chunk.is_valid().to_numpy(False)
            for i in range(len(chunk)):
                if not validity[i]:
                    out[offset + i] = None
                else:
                    sublist = values[offsets[i] : offsets[i + 1]]
                    out[offset + i] = (
                        (not any(combined_regex.search(x) for x in sublist))
                        if invert
                        else (any(combined_regex.search(x) for x in sublist))
                    )
        else:
            validity = chunk.is_valid().to_numpy(False)
            strings = chunk.to_pylist()
            for i in range(len(chunk)):
                if not validity[i]:
                    out[offset + i] = None
                else:
                    is_match = combined_regex.search(strings[i]) is not None
                    out[offset + i] = not is_match if invert else is_match
        offset += len(chunk)

    return out


def convert_camel_to_sql_case(s: str) -> str:
    """
    Convert a PascalCase or camelCase string to an SQL-style uppercase string with spaces.

    Parameters:
        s: str
            The input string in PascalCase or camelCase.

    Returns:
        str: The converted string in SQL format.
    """
    return re.sub(r"([A-Z])", r" \1", s).strip().upper()
