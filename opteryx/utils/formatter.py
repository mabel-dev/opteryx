import re


def tokenize_string(string):
    # Define the regular expression to match tokens
    token_pattern = re.compile(r"[(),]|[^\s(),]+")

    # Use the regular expression to find all tokens in the string
    tokens = token_pattern.findall(string)

    # Remove any leading or trailing whitespace from the tokens
    tokens = [token.strip() for token in tokens]

    return tokens


def format_sql(sql):
    """
    Adds colorization to SQL statements to make it easier to find keywords and literals
    """
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
            "INNER",
            "JOIN",
            "LEFT",
            "LIMIT",
            "ON",
            "ORDER",
            "OUTER",
            "RIGHT",
            "SELECT",
            "SET",
            "SHOW",
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
        else:
            formatted_sql += word + " "

    formatted_sql += "\033[0m"

    formatted_sql = formatted_sql.replace(" \033[38;5;102m(", "\033[38;5;102m(")
    formatted_sql = formatted_sql.replace("(\033[0m ", "(\033[0m")
    formatted_sql = formatted_sql.replace(" \033[38;5;102m)", "\033[38;5;102m)")
    formatted_sql = formatted_sql.replace(" \033[38;5;102m,", "\033[38;5;102m,")
    formatted_sql = formatted_sql.replace(" \033[38;5;102m;", "\033[38;5;102m;")

    return formatted_sql.strip()
