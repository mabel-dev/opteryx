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
    """get the element from a list which follows a given element"""
    try:
        index = lst.index(item)
        return lst[index + 1]
    except IndexError:
        return None


def _extract_as_at(path):
    for part in path.split("/"):
        if part.startswith("as_at_"):
            return part
    return ""


def _extract_by(path):
    for part in path.split("/"):
        if part.startswith("by_"):
            return part


_is_complete = lambda blobs, as_at: any(
    blob for blob in blobs if as_at + "/frame.complete" in blob
)
_is_invalid = lambda blobs, as_at: any(
    blob for blob in blobs if (as_at + "/frame.ignore" in blob)
)


class MabelPartitionScheme(BasePartitionScheme):
    """
    Handle reading data using the Mabel partition scheme.
    """

    def partition_format(self):
        return "year_{yyyy}/month_{mm}/day_{dd}"

    def _inner_filter_blobs(self, list_of_blobs, statistics):

        # The segments are stored in folders with the prefix 'by_', as in,
        # segments **by** field name
        list_of_segments = {
            _extract_by(blob) for blob in list_of_blobs if "/by_" in blob
        }
        chosen_segment = ""

        # If we have multiple 'by_' segments, pick one - pick the first one until
        # we start making cost-based decisions
        if list_of_segments:
            list_of_segments = sorted(list_of_segments)
            chosen_segment = list_of_segments.pop()
            # Do the pruning
            list_of_blobs = [
                blob for blob in list_of_blobs if f"/{chosen_segment}/" in blob
            ]

        # build a list of the segments we're going to read, for example, if we have
        # data which are segmented by hour, this will be the hour=00 part
        if chosen_segment == "":
            segmented_folders = {""}
        else:
            segmented_folders = {
                _safe_get_next_element(blob.split("/"), chosen_segment)
                for blob in list_of_blobs
            }

        # count the segments we're planning to read
        statistics.segments_scanned += len(segmented_folders)

        # go through the list of segments, getting the active frame for each
        for segment_folder in segmented_folders:

            # we get the blobs for this segment by looking for the path to contain
            # a combination of the segment and the segmented folder
            segment_blobs = [
                blob
                for blob in list_of_blobs
                if f"{chosen_segment}/{segment_folder}" in blob
            ]

            # work out if there's an as_at part
            as_ats = {
                _extract_as_at(blob) for blob in segment_blobs if "as_at_" in blob
            }
            if as_ats:
                as_ats = sorted(as_ats)
                as_at = as_ats.pop()

                while not _is_complete(segment_blobs, as_at) or _is_invalid(
                    segment_blobs, as_at
                ):
                    if len(as_ats) > 0:
                        as_at = as_ats.pop()
                    else:
                        return []

                # get_logger().debug(f"Reading Frame `{as_at}`")
                yield from (blob for blob in segment_blobs if (as_at in blob))

            else:
                yield from list_of_blobs

    def filter_blobs(self, list_of_blobs, statistics):
        return list(self._inner_filter_blobs(list_of_blobs, statistics))
