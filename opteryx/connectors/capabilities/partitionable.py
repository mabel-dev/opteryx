# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


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
