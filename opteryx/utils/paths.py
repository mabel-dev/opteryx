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

"""
Functions to help with handling file paths
"""
import os

OS_SEP = os.sep


def get_parts(path_string: str):

    # Validate against path traversal and home directory references
    if ".." in path_string or path_string.startswith("~"):
        raise ValueError(
            "get_parts: paths cannot traverse the folder structure or use home directory shortcuts"
        )

    # Split the path into parts
    parts = path_string.split(OS_SEP)

    # Handle Windows paths which may contain drive letters
    bucket = ""
    if len(parts) > 1:
        bucket = parts.pop(0)

    # Identify if the last part contains a filename with an extension
    if "." in parts[-1]:
        file_name_part = parts.pop(-1)
        file_name, suffix = file_name_part.rsplit(".", 1)
        suffix = "." + suffix  # Prepend '.' to ensure the suffix starts with a dot
    else:
        file_name = ""
        suffix = ""

    parts_path = OS_SEP.join(parts)

    return bucket, parts_path, file_name, suffix
