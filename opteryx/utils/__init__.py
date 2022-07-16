import itertools

try:
    # added 3.9
    from functools import cache
except ImportError:
    from functools import lru_cache

    cache = lru_cache(1)


@cache
def is_running_from_ipython():
    """
    True when running in Jupyter
    """
    try:
        from IPython import get_ipython  # type:ignore

        return get_ipython() is not None
    except:
        return False


def peak(generator):
    """
    peak an item off a generator, this may have undesirable consequences so
    only use if you also wrote the generator
    """
    try:
        item = next(generator)
    except StopIteration:
        return None
    return item, itertools.chain(item, generator)
