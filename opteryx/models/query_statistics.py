# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.


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
        if attr == "messages" and "messages" not in self._stats:
            return []
        return self._stats[attr]

    def __setattr__(self, attr, value):
        if attr == "_stats":
            super().__setattr__(attr, value)
        else:
            self._stats[attr] = value

    def increase(self, attr: str, amount: float):
        self._stats[attr] += amount

    def add_message(self, message: str):
        """collect warnings"""
        if "messages" not in self._stats:
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
        stats_dict = {key: stats_dict[key] for key in sorted(stats_dict)}
        stats_dict["messages"] = stats_dict.pop("messages", [])
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
            if len(cls._instances.keys()) > 10:
                # find the first key that is not "system"
                key_to_remove = next((key for key in cls._instances if key != "system"), None)
                if key_to_remove:
                    cls._instances.pop(key_to_remove)
        return cls._instances[qid]
