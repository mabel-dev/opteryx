try:
    from .sqloxide import parse_sql
except ImportError as e:  # pragma: no cover
    print(e)
    if str(e) != "PyO3 modules may only be initialized once per interpreter process":
        raise e

__all__ = ["parse_sql"]
