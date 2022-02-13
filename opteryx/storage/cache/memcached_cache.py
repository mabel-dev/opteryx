from functools import lru_cache


@lru_cache(1)
def memcached_server():
    import os

    # the server must be set in the environment
    memcached_config = os.environ.get("MEMCACHED_SERVER", None)
    if memcached_config is None:
        return None

    # expect either SERVER or SERVER:PORT entries
    memcached_config = memcached_config.split(":")
    if len(memcached_config) == 1:
        # the default memcached port
        memcached_config.append(11211)

    # we need the server and the port
    if len(memcached_config) != 2:
        return None

    try:
        from pymemcache.client import base
    except ImportError:
        return None

    # wait 1 second to try to connect, it's not worthwhile as a cache if it's slow
    return base.Client(
        (
            memcached_config[0],
            memcached_config[1],
        ),
        connect_timeout=1,
        timeout=1,
    )
