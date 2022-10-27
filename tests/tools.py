import io

from functools import wraps


def is_raspberry_pi():
    try:
        with io.open("/sys/firmware/devicetree/base/model", "r") as model:
            if "raspberry pi" in model.read().lower():
                return True
    except Exception:
        pass
    return False


def skip(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        print(f"Skipping {func.__name__}")

    return wrapper


def skip_on_raspberry_pi(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        if is_raspberry_pi():
            print(f"Skipping {func.__name__} - doesn't run on Raspberry Pi")
        else:
            return func(*args, **kwargs)

    return wrapper
