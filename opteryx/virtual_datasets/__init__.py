# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

import opteryx.virtual_datasets.astronaut_data as astronauts
import opteryx.virtual_datasets.derived_data as derived
import opteryx.virtual_datasets.no_table_data as no_table
import opteryx.virtual_datasets.planet_data as planets
import opteryx.virtual_datasets.satellite_data as satellites
import opteryx.virtual_datasets.variables_data as variables
from opteryx.virtual_datasets import missions
from opteryx.virtual_datasets import statistics
from opteryx.virtual_datasets import stop_words
from opteryx.virtual_datasets import user


def load_virtual_dataset(name: str):
    _DATASETS = {
        "$missions": "missions.parquet.zst",
        "$satellites": "satellites.parquet.zst",
        "$astronauts": "astronauts.parquet.zst",
    }

    import importlib.resources
    import io

    import pyarrow.parquet as pq
    import zstandard as zstd

    def _load_fallback_file(fname):
        from pathlib import Path

        # Fallback for source checkout mode
        here = Path(__file__).parent
        return (here / fname).read_bytes()

    fname = _DATASETS[name]
    try:
        with importlib.resources.files("opteryx.virtual_datasets").joinpath(fname).open("rb") as f:
            compressed = f.read()
    except (FileNotFoundError, AttributeError):
        compressed = _load_fallback_file(fname)

    decompressed = zstd.ZstdDecompressor().decompress(compressed)
    return pq.read_table(io.BytesIO(decompressed))
