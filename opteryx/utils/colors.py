COLORS = {
    "\001OFFm": "\033[0m",  # Text Reset
    # Opteryx named colors
    "\001PUNCm": "\033[38;5;102m",
    "\001VARCHARm": "\033[38;5;229m",
    "\001CONSTm": "\033[38;5;117m",
    "\001NULLm": "\033[38;5;102m",
    "\001VALUEm": "\033[38;5;153m",
    "\001NUMERICm": "\033[38;5;212m",
    "\001DATEm": "\033[38;5;120m",
    "\001TIMEm": "\033[38;5;72m",
    "\001KEYm": "\033[38;5;183m",
    # an orange color - 222
    # a red color = 209
    # Regular Colors
    "\001BLACKm": "\033[0;30m",  # Black
    "\001REDm": "\033[0;31m",  # Red
    "\001GREENm": "\033[0;32m",  # Green
    "\001YELLOWm": "\033[0;33m",  # Yellow
    "\001BLUEm": "\033[0;34m",  # Blue
    "\001PURPLEm": "\033[0;35m",  # Purple
    "\001CYANm": "\033[0;36m",  # Cyan
    "\001WHITEm": "\033[0;37m",  # White
    # Bold
    "\001BOLD_BLACKm": "\033[1;30m",  # Black
    "\001BOLD_REDm": "\033[1;31m",  # Red
    "\001BOLD_GREENm": "\033[1;32m",  # Green
    "\001BOLD_YELLOWm": "\033[1;33m",  # Yellow
    "\001BOLD_BLUEm": "\033[1;34m",  # Blue
    "\001BOLD_PURPLEm": "\033[1;35m",  # Purple
    "\001BOLD_CYANm": "\033[1;36m",  # Cyan
    "\001BOLD_WHITEm": "\033[1;37m",  # White
    # Underline
    "\001UNDERLINE_BLACKm": "\033[4;30m",  # Black
    "\001UNDERLINE_REDm": "\033[4;31m",  # Red
    "\001UNDERLINE_GREENm": "\033[4;32m",  # Green
    "\001UNDERLINE_YELLOWm": "\033[4;33m",  # Yellow
    "\001UNDERLINE_BLUEm": "\033[4;34m",  # Blue
    "\001UNDERLINE_PURPLEm": "\033[4;35m",  # Purple
    "\001UNDERLINE_CYANm": "\033[4;36m",  # Cyan
    "\001UNDERLINE_WHITEm": "\033[4;37m",  # White
    # Background
    "\001BACKGROUND_BLACKm": "\033[40m",  # Black
    "\001BACKGROUND_REDm": "\033[41m",  # 	Red
    "\001BACKGROUND_GREE}m": "\033[42m",  # 	Green
    "\001BACKGROUND_YELLOWm": "\033[43m",  # 	Yellow
    "\001BACKGROUND_BLUEm": "\033[44m",  # 	Blue
    "\001BACKGROUND_PURPLEm": "\033[45m",  # 	Purple
    "\001BACKGROUND_CYANm": "\033[46m",  # 	Cyan
    "\001BACKGROUND_WHITEm": "\033[47m",  # 	White
}


def colorize(record, can_colorize=True):
    if can_colorize:
        for k, v in COLORS.items():
            record = record.replace(k, v)
    else:
        for k, v in COLORS.items():  # pragma: no cover
            record = record.replace(k, "")

    return record
