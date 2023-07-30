class Cacheable:
    """
    Read Thru Cache
    """

    def __init__(self, **kwargs):
        from opteryx.shared import BufferPool

        self.buffer_pool = BufferPool()

    def read_thru(self):
        """
        Decorator function for read-through cache functionality
        """

        def decorator(func):
            def wrapper(*args, **kwargs):
                # Assume args[0] is the key for simplicity; adjust as needed
                key = args[0]

                # Try to get the result from cache
                result = self.buffer_pool.get(key, None)

                if result is None:
                    # Key is not in cache, execute the function and store the result in cache
                    result = func(*args, **kwargs)

                    # Write the result to cache
                    self.buffer_pool.set(key, result, None)

                    print("MISS")
                else:
                    print("HIT")

                return result

            return wrapper

        return decorator
