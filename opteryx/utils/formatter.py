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
    # Split the SQL statement into individual words
    # this is not meant to cover all scenarios, it's mainly for debugging
    words = tokenize_string(sql)

    # Add spaces and new lines to format the SQL statement
    formatted_sql = ""
    indent_level = 0
    for word in words:
        if word == "(":
            indent_level += 1
            formatted_sql += "\033[38;5;102m" + word + "\033[0m "
        elif word == ")":
            formatted_sql += "\033[38;5;102m" + word + "\033[0m "
            indent_level -= 1
        elif word in [
            "SELECT",
            "FROM",
            "WHERE",
            "JOIN",
            "ON",
            "AND",
            "OR",
            "GROUP",
            "BY",
            "ORDER",
            "LIMIT",
        ]:
            formatted_sql += "\033[38;5;117m\n" + word.rjust(7) + "\033[0m "
        elif word in ["=", ">=", "<=", "!=", "<", ">", "<>"]:
            formatted_sql += "\033[38;5;183m" + word + "\033[0m "
        elif word.isdigit():
            formatted_sql += "\033[0;31m" + word + "\033[0m "
        else:
            formatted_sql += word + " "

    return "\n" + formatted_sql.strip()
