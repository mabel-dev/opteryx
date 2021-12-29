from functools import lru_cache


@lru_cache(1)
def is_running_from_ipython():
    """
    True when running in Jupyter
    """
    try:
        from IPython import get_ipython  # type:ignore

        return get_ipython() is not None
    except Exception:
        return False
