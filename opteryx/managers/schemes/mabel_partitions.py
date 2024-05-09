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
from opteryx.utils.dates import date_range
from opteryx.utils.file_decoders import DATA_EXTENSIONS

OS_SEP = os.sep


class UnsupportedSegementationError(DataError):
    """Exception raised for unsupported segmentations."""

    def __init__(self, dataset: str, segment: str):
        self.dataset = dataset
        self.segment = segment
        message = f"'{dataset}' contains unsupported segmentation (found `{segment}`), only 'by_hour' segments are supported."
        super().__init__(message)


def extract_prefix(path, prefix):
    start_index = path.find(prefix)
    end_index = path.find(OS_SEP, start_index) if start_index != -1 else -1
    return path[start_index:end_index] if end_index != -1 else None


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

        def process_as_ats(blobs, as_at_label, control_blobs):
            as_ats = sorted(
                {extract_prefix(blob, "as_at_") for blob in blobs if as_at_label in blob}
            )
            if len(as_ats) == 0:
                return blobs
            valid_blobs = []
            while as_ats:
                as_at = as_ats.pop()
                if as_at is None:
                    continue
                if is_complete_and_not_invalid(control_blobs, as_at):
                    valid_blobs = [blob for blob in blobs if as_at in blob]
                    break
                else:
                    blobs = [blob for blob in blobs if as_at not in blob]
            return valid_blobs

        def _inner(*, date, start, end):
            date_path = f"{prefix}{OS_SEP}year_{date.year:04d}{OS_SEP}month_{date.month:02d}{OS_SEP}day_{date.day:02d}"
            by_label = f"{OS_SEP}by_"
            as_at_label = f"{OS_SEP}as_at"

            # Call your method to get the list of blob names
            blob_names = blob_list_getter(prefix=date_path)
            if len(blob_names) == 0:
                return []

            control_blobs: List[str] = []
            data_blobs: List[str] = []

            for blob in blob_names:
                ext = os.path.splitext(blob)[1]
                if ext not in DATA_EXTENSIONS:
                    control_blobs.append(blob)
                else:
                    data_blobs.append(blob)
                    if by_label in blob:
                        segment = extract_prefix(blob, "by_")
                        if segment != "by_hour":
                            raise UnsupportedSegementationError(dataset=prefix, segment=segment)

            if any(f"{OS_SEP}by_hour{OS_SEP}" in blob_name for blob_name in data_blobs):

                start = min(start, date)
                end = max(end, date)

                selected_blobs = []

                for hour in date_range(start, end, "1h"):
                    hour_label = f"{OS_SEP}by_hour{OS_SEP}hour={hour.hour:02d}/"
                    # Filter for the specific hour, if hour folders exist
                    if any(hour_label in blob_name for blob_name in data_blobs):
                        hours_blobs = [
                            blob_name for blob_name in data_blobs if hour_label in blob_name
                        ]
                        selected_blobs.extend(
                            process_as_ats(hours_blobs, as_at_label, control_blobs)
                        )
            else:
                selected_blobs = process_as_ats(data_blobs, "as_at_", control_blobs)

            return selected_blobs

        start_date = start_date or midnight
        end_date = end_date or midnight.replace(hour=23, minute=59)

        found = set()

        # Use a ThreadPoolExecutor to parallelize fetching blobs for each hour
        with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
            # Prepare a list of future tasks
            futures = [
                executor.submit(_inner, **{"date": date, "start": start_date, "end": end_date})
                for date in date_range(start_date, end_date, "1d")
            ]
            # Wait for all futures to complete and collect results
            for future in concurrent.futures.as_completed(futures):
                found.update(future.result())

        return sorted(found)
