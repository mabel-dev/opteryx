"""
These are a set of functions that can be applied to data as it passes through.

These are the function definitions, the processor which uses these is in the
'inline_evaluator' module.
"""
from os import truncate
import orjson
import datetime
import fastnumbers
from siphashc import siphash
from functools import lru_cache

from mabel.utils.text import levenshtein_distance
from mabel.utils.dates import parse_iso


def get_year(input):
    """
    Convert input to a datetime object and extract the Year part
    """
    if isinstance(input, str):
        input = parse_iso(input)
    if isinstance(input, (datetime.date, datetime.datetime)):
        return input.year
    return None  #


def get_month(input):
    """
    Convert input to a datetime object and extract the Month part
    """
    if isinstance(input, str):
        input = parse_iso(input)
    if isinstance(input, (datetime.date, datetime.datetime)):
        return input.month
    return None


def get_day(input):
    """
    Convert input to a datetime object and extract the Day part
    """
    if isinstance(input, str):
        input = parse_iso(input)
    if isinstance(input, (datetime.date, datetime.datetime)):
        return input.day
    return None


def get_date(input):
    """
    Convert input to a datetime object and extract the Date part
    """
    if isinstance(input, str):
        input = parse_iso(input)
    if isinstance(input, (datetime.date, datetime.datetime)):
        return input.date()
    return None


def get_time(input):
    """
    Convert input to a datetime object and extract the Time part
    """
    if isinstance(input, str):
        input = parse_iso(input)
    if isinstance(input, (datetime.date, datetime.datetime)):
        return input.time()
    return None


def get_quarter(input):
    """
    Convert input to a datetime object and calculate the Quarter of the Year
    """
    if isinstance(input, str):
        input = parse_iso(input)
    if isinstance(input, (datetime.date, datetime.datetime)):
        return ((input.month - 1) // 3) + 1
    return None


def get_hour(input):
    """
    Convert input to a datetime object and extract the Hour part
    """
    if isinstance(input, str):
        input = parse_iso(input)
    if isinstance(input, (datetime.date, datetime.datetime)):
        return input.hour
    return None


def get_minute(input):
    """
    Convert input to a datetime object and extract the Minute part
    """
    if isinstance(input, str):
        input = parse_iso(input)
    if isinstance(input, (datetime.date, datetime.datetime)):
        return input.minute
    return None


def get_second(input):
    """
    Convert input to a datetime object and extract the Seconds part
    """
    if isinstance(input, str):
        input = parse_iso(input)
    if isinstance(input, (datetime.date, datetime.datetime)):
        return input.second
    return None


def get_week(input):
    """
    Convert input to a datetime object and extract the ISO8601 Week
    """
    if isinstance(input, str):
        input = parse_iso(input)
    if isinstance(input, (datetime.date, datetime.datetime)):
        return int(input.strftime("%V"))
    return None


def get_random():
    from ....utils.entropy import random_range

    return random_range(0, 999) / 1000


def concat(*items):
    """
    Turn each item to a string and concatenate the strings together
    """
    sep = ""
    if len(items) == 1 and (
        isinstance(items[0], (list, tuple, set)) or hasattr(items[0], "as_list")
    ):
        items = items[0]
        sep = ", "
    return sep.join(map(str, items))


def to_string(val):
    if isinstance(val, (list, tuple, set)):
        return concat(val)
    if hasattr(val, "mini"):
        return "\\" + val.mini.decode("UTF8") + "\\"
    if isinstance(val, dict):
        return "\\" + orjson.dumps(val).decode("UTF8") + "\\"
    else:
        return str(val)


def add_days(start_date, day_count):
    if isinstance(start_date, str):
        start_date = parse_iso(start_date)
    if isinstance(start_date, (datetime.date, datetime.datetime)):
        return start_date + datetime.timedelta(days=day_count)
    return None


def diff_days(start_date, end_date):
    if isinstance(start_date, str):
        start_date = parse_iso(start_date)
    if isinstance(end_date, str):
        end_date = parse_iso(end_date)
    if isinstance(start_date, (datetime.date, datetime.datetime)) and isinstance(
        end_date, (datetime.date, datetime.datetime)
    ):
        return (end_date - start_date).days
    return None


def parse_number(parser, coerce):
    def inner(val):
        if val is None:
            return None
        return coerce(parser(val))

    return inner


@lru_cache(8)
def get_md5(item):
    import hashlib

    return hashlib.md5(str(item).encode()).hexdigest()  # nosec - meant to be MD5


def attempt(func):
    try:
        func
    except:
        return None


FUNCTIONS = {
    # DATES & TIMES
    "YEAR": get_year,
    "MONTH": get_month,
    "DAY": get_day,
    "DATE": get_date,
    "QUARTER": get_quarter,
    "WEEK": get_week,
    "HOUR": get_hour,
    "MINUTE": get_minute,
    "SECOND": get_second,
    "TIME": get_time,
    "NOW": datetime.datetime.now,
    "ADDDAYS": add_days,
    "DAYSDIFF": diff_days,
    # STRINGS
    "UCASE": lambda x: str(x).upper(),
    "UPPER": lambda x: str(x).upper(),
    "LCASE": lambda x: str(x).lower(),
    "LOWER": lambda x: str(x).lower(),
    "TRIM": lambda x: str(x).strip(),
    "LEN": len,
    "STRING": to_string,
    "LEFT": lambda x, y: str(x)[: int(y)],
    "RIGHT": lambda x, y: str(x)[-int(y) :],
    "MID": lambda x, y, z: str(x)[int(y) :][: int(z)],
    "CONCAT": concat,
    "LEVENSHTEIN": levenshtein_distance,
    # NUMBERS
    "ROUND": round,
    "TRUNC": parse_number(fastnumbers.real, truncate),
    "INT": parse_number(fastnumbers.real, int),
    "FLOAT": parse_number(fastnumbers.real, float),
    # BOOLEAN
    "BOOLEAN": lambda x: str(x).upper() != "FALSE",
    "ISNONE": lambda x: x is None,
    # HASHING & ENCODING
    "HASH": lambda x: format(siphash("INCOMPREHENSIBLE", str(x)), "X"),
    "MD5": get_md5,
    "RANDOM": get_random,  # return a random number 0-99
    # OTHER
    "BETWEEN": lambda val, low, high: low < val < high,
    "SORT": lambda x: sorted(x),
    "TRY": attempt,
}
