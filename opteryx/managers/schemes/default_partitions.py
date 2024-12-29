# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

import datetime
from typing import Callable
from typing import List
from typing import Optional

from opteryx.managers.schemes import BasePartitionScheme


class DefaultPartitionScheme(BasePartitionScheme):
    def get_blobs_in_partition(
        self,
        *,
        blob_list_getter: Callable,
        prefix: str,
        start_date: Optional[datetime.datetime],
        end_date: Optional[datetime.datetime],
        **kwargs,
    ) -> List[str]:
        return blob_list_getter(prefix=prefix)
