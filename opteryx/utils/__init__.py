from functools import lru_cache


@lru_cache(1)
def is_running_from_ipython():
    """
    True when running in Jupyter
    """
    try:
        from IPython import get_ipython  # type:ignore

        return get_ipython() is not None
    except:
        return False


def safe_field_name(field_name):
    """strip all the non-alphanums from a field name"""
    import re

    pattern = re.compile(r"[^a-zA-Z0-9\_\-]+")
    return pattern.sub("", field_name)
