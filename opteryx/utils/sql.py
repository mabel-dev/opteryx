import re
from typing import List

import numpy

ESCAPE_SPECIAL_CHARS = re.compile(r"([.^$*+?{}[\]|()\\])")


def sql_like_to_regex(pattern: str) -> str:
    """
    Converts an SQL `LIKE` pattern into a regular expression.

    SQL `LIKE` syntax:
    - `%` matches zero or more characters (similar to `.*` in regex).
    - `_` matches exactly one character (similar to `.` in regex).
    - Special regex characters are escaped to ensure literal matching.

    Args:
        pattern (str): The SQL LIKE pattern.

    Returns:
        str: The equivalent regex pattern, anchored with `^` and `$`.

    Examples:
        sql_like_to_regex("a%")       -> "^a.*?$"
        sql_like_to_regex("_b")       -> "^.b$"
        sql_like_to_regex("%[test]%") -> "^.*?\[test\].*?$"
    """
    if pattern is None:
        raise ValueError("Pattern cannot be None")

    # Escape special regex characters in the pattern
    escaped_pattern = ESCAPE_SPECIAL_CHARS.sub(r"\\\1", pattern)

    # Replace SQL wildcards with regex equivalents
    regex_pattern = "^" + escaped_pattern.replace("%", ".*?").replace("_", ".") + "$"
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
    arr: numpy.ndarray,
    patterns: List[str],
    flags: int = 0,
    invert: bool = False,
) -> numpy.ndarray:
    """
    Evaluates whether each row in `arr` matches ANY of the given LIKE patterns.
    Patterns are converted to regexes, combined, and compiled once.

    Parameters:
        arr: numpy.ndarray
            1D array of rows. Each element can be:
                - None
                - A single string/bytes
                - A list/tuple/array of strings/bytes
              (all non-None elements are assumed to be the same structure).
        patterns: List[str]
            A list of SQL LIKE patterns. These get combined into a single regex.
        flags: int, optional
            Flags to pass to `re.compile()`, e.g. re.IGNORECASE for ILIKE.

    Returns:
        numpy.ndarray:
            A 1D object array with True, False, or None,
            indicating whether each row did (or did not) match the patterns.
    """
    if any(not isinstance(p, str) for p in patterns if p):
        from opteryx.exceptions import IncorrectTypeError

        raise IncorrectTypeError("Patterns for LIKE ANY comparisons must be strings.")

    # 1) Combine the LIKE patterns into a single compiled regex
    #    (Empty patterns list => empty string => matches nothing)
    combined_pattern_str = r"|".join(sql_like_to_regex(p) for p in patterns if p)
    # If there are no valid patterns, we build a "never match" pattern
    if not combined_pattern_str:
        combined_pattern_str = r"(?!x)"  # Negative lookahead to never match

    combined_regex = re.compile(combined_pattern_str, flags=flags)

    # 2) Create the output array (dtype=object so we can store None/bool)
    out = numpy.empty(arr.size, dtype=object)

    # 3) Determine if the array consists of single strings or lists-of-strings
    first_non_none = None
    for x in arr:
        if x is not None:
            first_non_none = x
            break

    # If the entire array is None, just return all None
    if first_non_none is None:
        out[:] = None
        return out

    single_string_mode = isinstance(first_non_none, (str, bytes))

    # 4) Main loop
    if single_string_mode:
        # Single-string mode
        for i, row in enumerate(arr):
            if row is None:
                out[i] = None
            else:
                # Match or not?
                is_match = combined_regex.search(row) is not None
                out[i] = (not is_match) if invert else is_match
    else:
        # Lists-of-strings mode
        for i, row in enumerate(arr):
            if row is None:
                out[i] = None
            else:
                # row is assumed to be an iterable of strings/bytes
                if row.size == 0:
                    # Probably a numpy array with zero length
                    is_match = False
                else:
                    # If anything in the row matches, it's True
                    is_match = any(combined_regex.search(elem) for elem in row)
                out[i] = (not is_match) if invert else is_match

    return out
