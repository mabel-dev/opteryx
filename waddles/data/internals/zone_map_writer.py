"""
ZoneMap Writer

This is a combined index (BRIN) and data profiler.

As an Index:

  This is a Block Range Index - also known as a MinMax Index - this records the maximum
  and minimum values for each attribute in each block (in this case Blobs). This is
  used to determine if a specific Blob has rows which will satisfy a selection.

  A BRIN records the maximum and minimum values of each attribute, if we're using a
  selection in our query, we can quickly identify if a Blob definitely doesn't have a
  matching row if it's between the minimum and maximum values. Not being ruled out
  doesn't mean the value is is the Blob, as such this is a probabilistic approach.

As a Data Profiler:

  This contains a limited selection of information about the dataset, the purpose of
  capturing this information is to provide information to the index and query planners
  such as cardinality of records and if columns contain nulls.

  The profiler also contains description information from the Schema, if provided to
  the reader.
  
  This data can also be used as part of a higher-level profiler.

The resultant map file is a JSON file / Python dict:

{
    "blob_name": {
        "column_name": {
            profile information
        },
        "column_name": {
            profile_information
        }
    },
    "blob_name": {
        blob_information
    }
}
  
"""
import datetime
import json
from typing import Any, Optional
from mabel.data.internals.algorithms.hyper_log_log import HyperLogLog
from mabel.data.internals.attribute_domains import MABEL_TYPES, get_coerced_type
from mabel.data.internals.schema_validator import Schema


HYPERLOGLOG_ERROR_RATE = 0.005


class ZoneMap:

    slots = (
        "type",
        "minimum",
        "maximum",
        "count",
        "missing",
        "cumulative_sum",
        "cardinality",
        "description",
    )

    def __init__(self):
        self.type: str = "unknown"
        self.minimum: Any = None
        self.maximum: Any = None
        self.count: int = 0
        self.missing: int = 0
        self.cumulative_sum: float = 0
        self.cardinality: float = 0
        self.description: str = "none"

    def as_dict(self):
        return {
            "type": get_coerced_type(self.type),
            "minimum": self.minimum,
            "maximum": self.maximum,
            "count": self.count,
            "missing": self.missing,
            "cumulative_sum": self.cumulative_sum,
            "description": self.description,
            "cardinality": self.cardinality,
        }


class ZoneMapWriter(object):
    def __init__(self, schema: Optional[Schema] = None):
        self.collectors = {}
        self.hyper_log_logs = {}
        self.record_counter = 0

        # raise NotImplementedError("ZoneMapWriter needs some refactoring")

        # extract type and desc info from the schema

    def add(self, row, blob):
        # count every time we've been called - this is the total record count for
        # the partition
        self.record_counter += 1

        if not blob in self.collectors:
            self.collectors[blob] = {"*record_counter": 1}
        else:
            self.collectors[blob]["*record_counter"] += 1

        for k, v in row.items():

            if k in self.collectors[blob]:
                collector = self.collectors[blob][k]
            else:
                collector = ZoneMap()
                # we don't want to put the HLL in the ZoneMap, so create
                # a sidecar HLL which we dispose of later.
                self.hyper_log_logs[f"{blob}:{k}"] = HyperLogLog(HYPERLOGLOG_ERROR_RATE)
            collector.count += 1

            # if the value is missing, count it and skip everything else
            if v is None and v != False:
                continue

            # calculate the min/max for ordinals (and strings) and the cummulative
            # sum for numerics
            value_type = type(v)
            if value_type == str:
                if len(v) > 128:
                    v = v[:128]
            if value_type in (int, float, str, datetime.date, datetime.datetime):
                if collector.maximum:
                    # this is faster than max(a,b)
                    collector.maximum = (
                        collector.maximum if collector.maximum >= v else v
                    )
                    collector.minimum = (
                        collector.minimum if collector.minimum <= v else v
                    )
                else:
                    collector.maximum = v
                    collector.minimum = v
            if value_type in (int, float):
                collector.cumulative_sum += v

            #            # track the type of the attribute, if it changes mark as mixed
            value_type_name = value_type.__name__
            if collector.type != value_type_name:
                if collector.type == "unknown":
                    collector.type = value_type_name
                else:
                    collector.type = "mixed"

            # count the unique items, use a hyper-log-log for size and speed
            # this gives us an estimate only.
            if value_type in (int, float, str, datetime.date, datetime.datetime):
                self.hyper_log_logs[f"{blob}:{k}"].add(v)

            # put the profile back in the collector
            self.collectors[blob][k] = collector

    def profile(self):
        for blob in self.collectors:
            for column in self.collectors[blob]:
                if column != "*record_counter":

                    self.collectors[blob][column].missing = (
                        self.collectors[blob]["*record_counter"]
                        - self.collectors[blob][column].count
                    )
                    # High cardinality (closer to 1) indicates a greated number of unique
                    # values. The error ratio for the HLL is 1/200, so we're going to round to
                    # the nearest 1/1000th
                    self.collectors[blob][column].cardinality = round(
                        self.hyper_log_logs[f"{blob}:{column}"].card()
                        / self.collectors[blob][column].count,
                        3,
                    )

                    self.collectors[blob][column] = self.collectors[blob][
                        column
                    ].as_dict()

        return json.dumps(self.collectors).encode()
