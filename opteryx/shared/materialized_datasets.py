# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Materialized Datasets.

Used to store datasets manually registered with the engine.

It's just a dictionary.
"""


class MaterializedDatasets(dict):
    _md: dict = None

    def __new__(cls):
        if cls._md is None:
            cls._md = {}
        return cls._md
