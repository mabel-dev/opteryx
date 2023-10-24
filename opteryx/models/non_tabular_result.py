from opteryx.constants import QueryStatus


class NonTabularResult:
    """
    Class to encapsulate non-tabular query results.
    """

    def __init__(self, record_count: int = None, status: QueryStatus = QueryStatus._UNDEFINED):
        self.record_count = record_count
        self.status = status
