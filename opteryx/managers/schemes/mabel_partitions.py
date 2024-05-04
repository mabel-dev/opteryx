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

import concurrent.futures
import datetime
import os
from typing import Callable
from typing import List
from typing import Optional

from opteryx.exceptions import DataError
from opteryx.managers.schemes import BasePartitionScheme
from opteryx.utils.file_decoders import DATA_EXTENSIONS

OS_SEP = os.sep


class UnsupportedSegementationError(DataError):
    """Exception raised for unsupported segmentations."""

    def __init__(self, dataset: str, segments: set = None):
        self.dataset = dataset
        self.segments = segments
        message = f"'{dataset}' contains unsupported segmentation (`{'`, `'.join(segments)}`), only 'by_hour' segments are supported."
        super().__init__(message)


def extract_prefix(path, prefix):
    start_index = path.find(prefix)
    if start_index == -1:
        return None
    end_index = path.find(OS_SEP, start_index)
    if end_index == -1:
        return None
    return path[start_index:end_index]


def is_complete_and_not_invalid(blobs, as_at):
    # Directly initialize flags to False.
    complete = False
    ignore = False

    # Search suffix strings are constant for each call, so define them once.
    complete_suffix = f"{as_at}{OS_SEP}frame.complete"
    invalid_suffix = f"{as_at}{OS_SEP}frame.ignore"

    # Iterate over blobs once, checking conditions.
    for blob in blobs:

        if complete_suffix in blob:
            complete = True
            if complete and ignore:
                break
        elif invalid_suffix in blob:
            ignore = True
            if complete and ignore:
                break

    return complete and not ignore


class MabelPartitionScheme(BasePartitionScheme):
    """
    Handle reading data using the Mabel partition scheme.
    """

    def get_blobs_in_partition(
        self,
        *,
        blob_list_getter: Callable,
        prefix: str,
        start_date: Optional[datetime.datetime],
        end_date: Optional[datetime.datetime],
    ) -> List[str]:
        """filter the blobs acording to the chosen scheme"""

        midnight = datetime.datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0)
        by_label = f"{OS_SEP}by_"
        as_at_label = f"{OS_SEP}as_at"

        def _inner(*, timestamp):
            date_path = f"{prefix}{OS_SEP}year_{timestamp.year:04d}{OS_SEP}month_{timestamp.month:02d}{OS_SEP}day_{timestamp.day:02d}"
            hour_label = f"{OS_SEP}by_hour{OS_SEP}hour={timestamp.hour:02d}/"

            # Call your method to get the list of blob names
            blob_names = blob_list_getter(prefix=date_path)
            if len(blob_names) == 0:
                return []

            control_blobs = []
            data_blobs = []
            segments = set()
            hour_blobs = []

            for blob in blob_names:
                extension = os.path.splitext(blob)[1]
                if extension not in DATA_EXTENSIONS:
                    control_blobs.append(blob)
                else:
                    data_blobs.append(blob)

                # Collect segments
                if by_label in blob:
                    segments.add(extract_prefix(blob, "by_"))

                    # Collect hour specific blobs
                    if hour_label in blob:
                        hour_blobs.append(blob)

            if hour_blobs:
                data_blobs = hour_blobs

            if segments - {"by_hour", None}:
                raise UnsupportedSegementationError(dataset=prefix, segments=segments)

            as_at = None
            as_ats = sorted(
                {extract_prefix(blob, "as_at_") for blob in blob_names if as_at_label in blob}
            )

            # Keep popping from as_ats until a valid frame is found
            while as_ats:
                as_at = as_ats.pop()
                if as_at is None:
                    continue
                if is_complete_and_not_invalid(control_blobs, as_at):
                    data_blobs = (blob for blob in data_blobs if as_at in blob)  # type: ignore
                    break
                data_blobs = [blob for blob in data_blobs if as_at not in blob]
                as_at = None

            return data_blobs

        start_date = start_date or midnight
        end_date = end_date or midnight.replace(hour=23, minute=59)

        found = set()

        # Use a ThreadPoolExecutor to parallelize fetching blobs for each hour
        with concurrent.futures.ThreadPoolExecutor() as executor:
            # Prepare a list of future tasks
            futures = [
                executor.submit(_inner, **{"timestamp": ts})
                for ts in self.hourly_timestamps(start_date, end_date)
            ]
            # Wait for all futures to complete and collect results
            for future in concurrent.futures.as_completed(futures):
                found.update(future.result())

        return sorted(found)
