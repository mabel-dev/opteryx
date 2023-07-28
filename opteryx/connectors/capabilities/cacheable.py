"""
Supports read-through cache
"""

from functools import wraps


class Cacheable:
    def __init__(self):
        print("we need to set the cache")

    def read_thru(self, func):
        """
        Decorator function for read-through cache functionality
        """

        @wraps(func)
        def wrapper(*args, **kwargs):
            # Assume args[0] is the key for simplicity; adjust as needed
            key = args[0]

            # Try to get the result from cache
            result = self.cache.get(key)

            if result is None:
                # Key is not in cache, execute the function and store the result in cache
                result = func(*args, **kwargs)

                # Write the result to cache
                self.cache[key] = result

            return result

        return wrapper
