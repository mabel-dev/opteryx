# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""Virtual datasets package.

This package exposes several small built-in datasets. Importing the
individual dataset modules triggers imports of heavy libraries (pyarrow,
zstandard, etc.), so we lazily load those modules on first access.
"""

_MODULES = {
    "astronauts": "opteryx.virtual_datasets.astronaut_data",
    "derived": "opteryx.virtual_datasets.derived_data",
    "no_table": "opteryx.virtual_datasets.no_table_data",
    "planets": "opteryx.virtual_datasets.planet_data",
    "satellites": "opteryx.virtual_datasets.satellite_data",
    "variables": "opteryx.virtual_datasets.variables_data",
    "missions": "opteryx.virtual_datasets.missions",
    "statistics": "opteryx.virtual_datasets.statistics",
    "stop_words": "opteryx.virtual_datasets.stop_words",
    "user": "opteryx.virtual_datasets.user",
}


def __getattr__(name: str):
    """Lazily import and return submodules like `planets`, `missions`, etc.

    This allows code to reference `opteryx.virtual_datasets.planets` without
    importing pyarrow until the dataset module is actually used.
    """
    if name in _MODULES:
        import importlib

        module = importlib.import_module(_MODULES[name])
        globals()[name] = module
        return module
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


def load_virtual_dataset(name: str):
    _DATASETS = {
        "$missions": "missions.parquet.zst",
        "$satellites": "satellites.parquet.zst",
        "$astronauts": "astronauts.parquet.zst",
    }

    import importlib.resources
    import io

    import pyarrow.parquet as pq

    from opteryx.compiled.io import disk_reader
    from opteryx.third_party.facebook import zstd

    def _load_fallback_file(fname):
        from pathlib import Path

        # Fallback for source checkout mode
        here = Path(__file__).parent
        return (here / fname).read_bytes()

    fname = _DATASETS[name]
    decompressed = None
    mmap_obj = None
    try:
        with importlib.resources.path("opteryx.virtual_datasets", fname) as dataset_path:
            mmap_obj = disk_reader.read_file_mmap(str(dataset_path))
        decompressed = zstd.decompress(memoryview(mmap_obj))
    except (FileNotFoundError, AttributeError):
        compressed = _load_fallback_file(fname)
        decompressed = zstd.decompress(compressed)
    finally:
        if mmap_obj is not None:
            disk_reader.unmap_memory(mmap_obj)
    return pq.read_table(io.BytesIO(decompressed))
