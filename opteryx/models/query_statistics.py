# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.


from collections import defaultdict


class _QueryStatistics:
    def __init__(self):
        # predefine "messages" and "executed_plan" so all new statistics default to 0
        self._stats: dict = defaultdict(int)
        self._stats["messages"] = []
        self._stats["executed_plan"] = None

    def _ns_to_s(self, nano_seconds: int) -> float:
        """convert elapsed ns to s"""
        if nano_seconds == 0:
            return 0
        return nano_seconds / 1e9

    def __getattr__(self, attr):
        """allow access using stats.statistic_name"""
        return self._stats[attr]

    def __setattr__(self, attr, value):
        """allow access using stats.statistic_name"""
        if attr == "_stats":
            super().__setattr__(attr, value)
        else:
            self._stats[attr] = value

    def increase(self, attr: str, amount: float = 1.0):
        self._stats[attr] += amount

    def add_message(self, message: str):
        """collect warnings"""
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
        # sort the keys in the dictionary
        stats_dict = {key: stats_dict[key] for key in sorted(stats_dict)}
        # put messages and executed_plan at the end
        stats_dict["messages"] = stats_dict.pop("messages", [])
        stats_dict["executed_plan"] = stats_dict.pop("executed_plan", None)
        return stats_dict


class QueryStatistics(_QueryStatistics):
    slots = "_instances"

    _instances: dict[str, _QueryStatistics] = {}

    def __new__(cls, qid=""):
        if cls._instances.get(qid) is None:
            cls._instances[qid] = _QueryStatistics()
            if len(cls._instances.keys()) > 16:
                # find the first key that is not "system"
                key_to_remove = next((key for key in cls._instances if key != "system"), None)
                if key_to_remove:
                    cls._instances.pop(key_to_remove)
        return cls._instances[qid]
