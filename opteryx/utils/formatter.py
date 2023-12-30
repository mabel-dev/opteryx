import re


def tokenize_string(string):
    # Define the regular expression to match tokens
    token_pattern = re.compile(r"[(),;]|[\r\n]+|[^\s(),;\r\n]+")

    # Use the regular expression to find all tokens in the string
    tokens = token_pattern.findall(string)

    # Remove any leading or trailing whitespace from the tokens
    tokens = ["\n" if token == "\n" else token.strip() for token in tokens]

    return tokens


def format_sql(sql):  # pragma: no cover
    """
    Adds colorization to SQL statements to make it easier to find keywords and literals
    """

    def color_comments(string):  # pragma: no cover
        pattern = r"(\"[^\"]\"|\'[^\']\'|\`[^\`]\`)|(/\*[^\*/]*\*/|--[^\r\n]*$)"
        regex = re.compile(pattern, re.MULTILINE | re.DOTALL)
        ansi_escape = re.compile(r"(?:\x1B[@-_]|[\x80-\x9F])[0-?]*[ -/]*[@-~]")

        def _replacer(match):
            if match.group(2) is not None:
                return f"\033[38;2;98;114;164m\033[3m{ansi_escape.sub('', match.group(2))}\033[0m"
            return match.group(1)

        return regex.sub(_replacer, string)

    if hasattr(sql, "decode"):
        sql = sql.decode()
    words = tokenize_string(sql)

    formatted_sql = ""
    in_string_literal = False

    for i, word in enumerate(words):
        if not in_string_literal and word.startswith("'"):
            formatted_sql += "\033[38;2;255;171;82m" + word
            if word.endswith("'"):
                in_string_literal = False
                formatted_sql += "\033[0m "
            else:
                in_string_literal = True
        elif in_string_literal:
            formatted_sql += " " + word
            if word.endswith("'"):
                in_string_literal = False
                formatted_sql += "\033[0m "
        elif word in ("(", ")", ",", ";"):
            formatted_sql += "\033[38;5;102m" + word + "\033[0m "
        elif word.upper() in {
            "ANALYZE",
            "ANTI",
            "AS",
            "BY",
            "CROSS",
            "DATES",
            "DISTINCT",
            "EXPLAIN",
            "FOR",
            "FROM",
            "FULL",
            "GROUP",
            "HAVING",
            "INNER",
            "JOIN",
            "LEFT",
            "LIMIT",
            "OFFSET",
            "ON",
            "ORDER",
            "OUTER",
            "RIGHT",
            "SELECT",
            "SET",
            "SHOW",
            "SINCE",
            "TODAY",
            "UNION",
            "USE",
            "USING",
            "WHERE",
            "WITH",
            "YESTERDAY",
        }:
            formatted_sql += "\033[38;2;139;233;253m" + word.upper() + "\033[0m "
        elif word.upper() in ("TRUE", "FALSE", "NULL"):
            formatted_sql += "\033[38;2;255;184;108m" + word.upper() + "\033[0m "
        elif (i + 1) < len(words) and words[i + 1] == "(":
            formatted_sql += "\033[38;2;80;250;123m" + word.upper() + "\033[0m"
        elif word.upper() in (
            "=",
            ">=",
            "<=",
            "!=",
            "<",
            ">",
            "<>",
            "LIKE",
            "ILIKE",
            "RLIKE",
            "NOT",
            "AND",
            "OR",
            "IN",
            "SIMILAR",
            "TO",
            "BETWEEN",
            "IS",
        ):
            formatted_sql += "\033[38;2;189;147;249m" + word.upper() + "\033[0m "
        elif word.replace(".", "", 1).isdigit():
            formatted_sql += "\033[38;2;255;184;108m" + word + "\033[0m "
        elif word == "\n":
            formatted_sql = formatted_sql.strip() + "\n"
        else:
            formatted_sql += word + " "

    formatted_sql += "\033[0m"

    formatted_sql = formatted_sql.replace(" \033[38;5;102m(", "\033[38;5;102m(")
    formatted_sql = formatted_sql.replace("(\033[0m ", "(\033[0m")
    formatted_sql = formatted_sql.replace(" \033[38;5;102m)", "\033[38;5;102m)")
    formatted_sql = formatted_sql.replace(" \033[38;5;102m,", "\033[38;5;102m,")
    formatted_sql = formatted_sql.replace(" \033[38;5;102m;", "\033[38;5;102m;")

    return color_comments(formatted_sql).strip()
