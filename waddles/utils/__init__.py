from .ipython import is_running_from_ipython


def safe_field_name(field_name):
    """strip all the non-alphanums from a field name"""
    import re

    pattern = re.compile(r"[^a-zA-Z0-9\_\-]+")
    return pattern.sub("", field_name)
