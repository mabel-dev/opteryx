import datetime

from opteryx.exceptions import InvalidConfigurationError

ONE_HOUR = datetime.timedelta(hours=1)


class Partitionable:
    partitioned = True

    def __init__(self, **kwargs):
        partition_scheme = kwargs.get("partition_scheme")

        from opteryx.managers.schemes import BasePartitionScheme
        from opteryx.managers.schemes import DefaultPartitionScheme

        if partition_scheme is None:
            partition_scheme = DefaultPartitionScheme

        if not isinstance(partition_scheme, type):
            raise InvalidConfigurationError(
                config_item="partition_scheme",
                provided_value=str(partition_scheme.__class__.__name__),
                valid_value_description="an uninitialized class.",
            )

        if not issubclass(partition_scheme, BasePartitionScheme):
            raise InvalidConfigurationError(
                config_item="partition_scheme", provided_value=str(partition_scheme.__name__)
            )

        self.partition_scheme = partition_scheme()

        self.start_date = None
        self.end_date = None

    def hourly_timestamps(self, start_time: datetime.datetime, end_time: datetime.datetime):
        """
        Create a generator of timestamps one hour apart between two datetimes.
        """

        current_time = start_time.replace(minute=0, second=0, microsecond=0)
        while current_time <= end_time:
            yield current_time
            current_time += ONE_HOUR
