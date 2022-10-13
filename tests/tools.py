from functools import wraps

def skip(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        print(f"Skipping {func.__name__}")
    return wrapper