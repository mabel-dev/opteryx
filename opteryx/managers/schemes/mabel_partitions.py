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
    end_index = path.find(OS_SEP, start_index)
    return path[start_index : end_index if end_index != -1 else None]


def is_complete_and_not_invalid(blobs, as_at):
    complete_suffix = f"{as_at}{OS_SEP}frame.complete"
    invalid_suffix = f"{as_at}{OS_SEP}frame.ignore"

    frame_blobs = (
        (complete_suffix in blob, invalid_suffix in blob)
        for blob in blobs
        if f"{as_at}{OS_SEP}frame." in blob
    )

    first_item = next(frame_blobs, (False, False))

    complete, ignore = zip(first_item, *frame_blobs)

    return any(complete) and not any(ignore)


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

        def _inner(*, timestamp):
            year = timestamp.year
            month = timestamp.month
            day = timestamp.day
            hour = timestamp.hour

            hour_label = f"{OS_SEP}by_hour{OS_SEP}hour={hour:02d}/"
            date_path = (
                f"{prefix}{OS_SEP}year_{year:04d}{OS_SEP}month_{month:02d}{OS_SEP}day_{day:02d}"
            )

            # Call your method to get the list of blob names
            blob_names = blob_list_getter(prefix=date_path)

            segments = {
                extract_prefix(blob, "by_") for blob in blob_names if f"{OS_SEP}by_" in blob
            }
            if len(segments - {"by_hour", None}) > 0:
                raise UnsupportedSegementationError(dataset=prefix, segments=segments)

            # Filter for the specific hour, if hour folders exist - prefer by_hour segements
            if any(hour_label in blob_name for blob_name in blob_names):
                blob_names = [blob_name for blob_name in blob_names if hour_label in blob_name]

            as_at = None
            as_ats = sorted(
                {extract_prefix(blob, "as_at_") for blob in blob_names if f"{OS_SEP}as_at_" in blob}
            )

            # Keep popping from as_ats until a valid frame is found
            while as_ats:
                as_at = as_ats.pop()
                if is_complete_and_not_invalid(blob_names, as_at):
                    break
                else:
                    blob_names = [blob for blob in blob_names if as_at not in blob]
                as_at = None

            if as_at:
                yield from (
                    blob
                    for blob in blob_names
                    if as_at in blob and os.path.splitext(blob)[1] in DATA_EXTENSIONS
                )
            else:
                yield from (
                    blob for blob in blob_names if os.path.splitext(blob)[1] in DATA_EXTENSIONS
                )

        start_date = start_date or midnight
        end_date = end_date or midnight.replace(hour=23, minute=59)

        found = set()

        for segment_timeslice in self.hourly_timestamps(start_date, end_date):
            blobs = _inner(timestamp=segment_timeslice)
            found.update(blobs)

        return sorted(found)
