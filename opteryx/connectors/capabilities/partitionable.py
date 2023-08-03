from opteryx.exceptions import InvalidConfigurationError


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
