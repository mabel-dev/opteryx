def is_int(value):
    try:
        int_value = int(value)
        return True
    except ValueError:
        return False


def is_float(value):
    try:
        int_value = float(value)
        return True
    except ValueError:
        return False
