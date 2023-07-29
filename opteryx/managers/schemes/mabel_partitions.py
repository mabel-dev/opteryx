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

from opteryx.managers.schemes import BasePartitionScheme


def _safe_get_next_element(lst, item):
    """Get the element from a list which follows a given element, if it exists"""
    try:
        index = lst.index(item)
        return lst[index + 1]
    except IndexError:
        pass


def _extract_part_from_path(path, prefix):
    """Extract the part of the path starting with a specific prefix"""
    for part in path.split("/"):
        if part.startswith(prefix):
            return part


def _extract_as_at(path):
    return _extract_part_from_path(path, "as_at_")


def _extract_by(path):
    return _extract_part_from_path(path, "by_")


_is_complete = lambda blobs, as_at: any(blob for blob in blobs if as_at + "/frame.complete" in blob)
_is_invalid = lambda blobs, as_at: any(blob for blob in blobs if as_at + "/frame.ignore" in blob)


class MabelPartitionScheme(BasePartitionScheme):
    """
    Handle reading data using the Mabel partition scheme.
    """

    def partition_format(self):
        return "year_{yyyy}/month_{mm}/day_{dd}"

    def _inner_filter_blobs(self, list_of_blobs, statistics):
        list_of_segments = sorted({_extract_by(blob) for blob in list_of_blobs if "/by_" in blob})

        chosen_segment = list_of_segments.pop() if list_of_segments else ""

        # Prune the list of blobs based on the chosen segment
        list_of_blobs = [blob for blob in list_of_blobs if f"/{chosen_segment}/" in blob]

        segmented_folders = {
            _safe_get_next_element(blob.split("/"), chosen_segment) for blob in list_of_blobs
        }

        # Count the segments we're planning to read
        statistics.segments_scanned += len(segmented_folders)

        for segment_folder in segmented_folders:
            segment_blobs = [
                blob for blob in list_of_blobs if f"{chosen_segment}/{segment_folder}" in blob
            ]

            as_ats = sorted({_extract_as_at(blob) for blob in segment_blobs if "as_at_" in blob})

            # Keep popping from as_ats until a valid frame is found
            while as_ats:
                as_at = as_ats.pop()
                if _is_complete(segment_blobs, as_at) and not _is_invalid(segment_blobs, as_at):
                    break

            if as_ats:
                yield from (blob for blob in segment_blobs if as_at in blob)
            else:
                yield from list_of_blobs

    def filter_blobs(self, list_of_blobs, statistics):
        return [blob for blob in self._inner_filter_blobs(list_of_blobs, statistics)]
