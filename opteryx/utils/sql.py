import re
from typing import List

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
    pattern = r"(\"[^\"]*\"|\'[^\']*\')|(/\*[\s\S]*?\*/|--[^\r\n]*$)"

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
