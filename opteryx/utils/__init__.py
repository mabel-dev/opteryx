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


from enum import Enum
from itertools import permutations
from typing import Iterable
from typing import Optional

from opteryx.third_party.mbleven import compare


def suggest_alternative(value: str, candidates: Iterable[str]) -> Optional[str]:
    """
    Find closest match using a variation of Levenshtein Distance with additional
    handling for rearranging function name parts.

    This implementation:
    - Is limited to searching for distance less than three.
    - Is case insensitive and ignores non-alphanumeric characters.
    - Tries rearranging parts of the name if an exact or close match is not found
      in its original form.

    This function is designed for quickly identifying likely matches when a user
    is entering field or function names and may have minor typos, casing or
    punctuation mismatches, or even jumbled parts of the name.

    Parameters:
        value: str
            The value to find matches for.
        candidates: List[str]
            A list of candidate names to match against.

    Returns:
        Optional[str]: The best match found, or None if no match is found.
    """
    name = "".join(char for char in value if char.isalnum())
    best_match_column = None
    best_match_score = 100  # Large number indicates no match found yet.

    # Function to find the best match
    def find_best_match(name: str):
        nonlocal best_match_column, best_match_score
        for raw, candidate in ((ca, "".join(ch for ch in ca if ch.isalnum())) for ca in candidates):
            my_dist = compare(candidate.lower(), name.lower())
            if my_dist == 0:  # if we find an exact match, return that immediately
                return raw
            if 0 <= my_dist < best_match_score:
                best_match_score = my_dist
                best_match_column = raw

    # First, try to find a match with the original name
    result = find_best_match(name)
    if result:
        return result

    # If no match was found, and the name contains '_', try rearranging parts
    if "_" in value:
        parts = value.split("_")
        combinations = permutations(parts)
        for combination in combinations:
            rearranged_name = "_".join(combination)
            result = find_best_match(rearranged_name)
            if result:
                return result

    return best_match_column  # Return the best match found, or None if no suitable match is found.


def dataclass_to_dict(instance):
    if isinstance(instance, Enum):
        return instance.name
    elif hasattr(instance, "to_dict"):
        return instance.to_dict()
    elif hasattr(instance, "__dataclass_fields__"):
        return {k: dataclass_to_dict(getattr(instance, k)) for k in instance.__dataclass_fields__}
    elif isinstance(instance, (list, tuple)):
        return [dataclass_to_dict(k) for k in instance]
    else:
        return instance
