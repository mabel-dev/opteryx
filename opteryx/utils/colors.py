COLORS = {
    "{OFF}": "\033[0m",  # Text Reset
    # Regular Colors
    "{BLACK}": "\033[0;30m",  # Black
    "{RED}": "\033[0;31m",  # Red
    "{GREEN}": "\033[0;32m",  # Green
    "{YELLOW}": "\033[0;33m",  # Yellow
    "{BLUE}": "\033[0;34m",  # Blue
    "{PURPLE}": "\033[0;35m",  # Purple
    "{CYAN}": "\033[0;36m",  # Cyan
    "{WHITE}": "\033[0;37m",  # White
    # Bold
    "{BOLD_BLACK}": "\033[1;30m",  # Black
    "{BOLD_RED}": "\033[1;31m",  # Red
    "{BOLD_GREEN}": "\033[1;32m",  # Green
    "{BOLD_YELLOW}": "\033[1;33m",  # Yellow
    "{BOLD_BLUE}": "\033[1;34m",  # Blue
    "{BOLD_PURPLE}": "\033[1;35m",  # Purple
    "{BOLD_CYAN}": "\033[1;36m",  # Cyan
    "{BOLD_WHITE}": "\033[1;37m",  # White
    # Underline
    "{UNDERLINE_BLACK}": "\033[4;30m",  # Black
    "{UNDERLINE_RED}": "\033[4;31m",  # Red
    "{UNDERLINE_GREEN}": "\033[4;32m",  # Green
    "{UNDERLINE_YELLOW}": "\033[4;33m",  # Yellow
    "{UNDERLINE_BLUE}": "\033[4;34m",  # Blue
    "{UNDERLINE_PURPLE}": "\033[4;35m",  # Purple
    "{UNDERLINE_CYAN}": "\033[4;36m",  # Cyan
    "{UNDERLINE_WHITE}": "\033[4;37m",  # White
}


def colorize(record, can_colorize=True):
    if can_colorize:
        for k, v in COLORS.items():
            record = record.replace(k, v)
    else:
        for k, v in COLORS.items():  # pragma: no cover
            record = record.replace(k, "")
    return record
