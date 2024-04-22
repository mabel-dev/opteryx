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


from collections import defaultdict


class _QueryStatistics:
    def __init__(self):
        self._stats: dict = defaultdict(int)
        self._stats["messages"] = []

    def _ns_to_s(self, nano_seconds: int) -> float:
        """convert elapsed ns to s"""
        if nano_seconds == 0:
            return 0
        return nano_seconds / 1e9

    def __getattr__(self, attr):
        if attr == "messages":
            if not "messages" in self._stats:
                return []
        return self._stats[attr]

    def __setattr__(self, attr, value):
        if attr == "_stats":
            super().__setattr__(attr, value)
        else:
            self._stats[attr] = value

    def add_message(self, message: str):
        """collect warnings"""
        if not "messages" in self._stats:
            self._stats["messages"] = [message]
        else:
            self._stats["messages"].append(message)

    def as_dict(self):
        """
        Return statistics as a dictionary
        """
        stats_dict = dict(self._stats)
        for k, v in stats_dict.items():
            # times are recorded in ns but reported in seconds
            if k.startswith("time_"):
                stats_dict[k] = self._ns_to_s(v)
        stats_dict["time_total"] = self._ns_to_s(
            stats_dict.pop("end_time", 0) - stats_dict.pop("start_time", 0)
        )
        stats_dict["messages"] = stats_dict.get("messages", [])
        return stats_dict

    def copy(self):
        return self

    def __deepcopy__(self):
        return self


class QueryStatistics(_QueryStatistics):
    slots = "_instances"

    _instances: dict[str, _QueryStatistics] = {}

    def __new__(cls, qid=""):
        if cls._instances.get(qid) is None:
            cls._instances[qid] = _QueryStatistics()
            if len(cls._instances.keys()) > 50:
                # don't keep collecting these things
                cls._instances.pop(next(iter(cls._instances)))
        return cls._instances[qid]
