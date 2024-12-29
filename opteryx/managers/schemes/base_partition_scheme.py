# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

import datetime
from typing import Callable
from typing import List
from typing import Optional

ONE_HOUR = datetime.timedelta(hours=1)


class BasePartitionScheme:
    """Implement a partition scheme"""

    def hourly_timestamps(self, start_time: datetime.datetime, end_time: datetime.datetime):
        """
        Create a generator of timestamps one hour apart between two datetimes.
        """

        current_time = start_time.replace(minute=0, second=0, microsecond=0)
        while current_time <= end_time:
            yield current_time
            current_time += ONE_HOUR

    def get_blobs_in_partition(
        self,
        *,
        blob_list_getter: Callable,
        prefix: str,
        start_date: Optional[datetime.datetime],
        end_date: Optional[datetime.datetime],
        **kwargs,
    ) -> List[str]:
        """filter the blobs acording to the chosen scheme"""
        raise NotImplementedError()
