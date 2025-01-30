import os
import platform
from distutils.sysconfig import get_config_var
from typing import Any
from typing import Dict

import numpy
from Cython.Build import cythonize
from setuptools import Extension
from setuptools import find_packages
from setuptools import setup
from setuptools_rust import RustExtension

LIBRARY = "opteryx"


def is_mac():  # pragma: no cover
    return platform.system().lower() == "darwin"


COMPILE_FLAGS = ["-O2"] if is_mac() else ["-O2", "-march=native", "-fvisibility=default"]

# Dynamically get the default include paths
include_dirs = [numpy.get_include()]

# Get the C++ include directory
includedir = get_config_var('INCLUDEDIR')
if includedir:
    include_dirs.append(os.path.join(includedir, 'c++', 'v1'))  # C++ headers path

# Get the Python include directory
includepy = get_config_var('INCLUDEPY')
if includepy:
    include_dirs.append(includepy)

# Check if paths exist
include_dirs = [p for p in include_dirs if os.path.exists(p)]

def rust_build(setup_kwargs: Dict[str, Any]) -> None:
    setup_kwargs.update(
        {
            "rust_extensions": [RustExtension("opteryx.compute", "Cargo.toml", debug=False)],
            "zip_safe": False,
        }
    )


__author__ = "notset"
__version__ = "notset"
with open(f"{LIBRARY}/__version__.py", mode="r") as v:
    vers = v.read()
exec(vers)  # nosec

with open("README.md", mode="r", encoding="UTF8") as rm:
    long_description = rm.read()

try:
    with open("requirements.txt", "r") as f:
        required = f.read().splitlines()
except:
    with open(f"{LIBRARY}.egg-info/requires.txt", "r") as f:
        required = f.read().splitlines()

extensions = [
    Extension(
        name="opteryx.third_party.abseil.containers",
        sources=[
            "opteryx/compiled/third_party/abseil_containers.pyx",
            "third_party/abseil/absl/hash/internal/hash.cc",
            "third_party/abseil/absl/hash/internal/city.cc", 
            "third_party/abseil/absl/container/internal/raw_hash_set.cc",
            "third_party/abseil/absl/hash/internal/low_level_hash.cc"
            ],
        include_dirs=include_dirs + ["third_party/abseil"],
        language="c++",
        extra_compile_args=COMPILE_FLAGS + ["-std=c++17"],
        extra_link_args=["-Lthird_party/abseil"],  # Link Abseil library
    ),
    Extension(
        name="opteryx.third_party.cyan4973.xxhash",
        sources=[
            "opteryx/compiled/third_party/xxhash.pyx",
            "third_party/cyan4973/xxhash.c"
            ],
        include_dirs=include_dirs + ["third_party/cyan4973"],
        extra_compile_args=COMPILE_FLAGS,
        extra_link_args=["-Lthird_party/cyan4973"],  # Link Abseil library
    ),
    Extension(
        name="opteryx.third_party.mrabarnett.regex._regex",
        sources=[
            "third_party/mrabarnett/_regex.c",
            "third_party/mrabarnett/_regex_unicode.c"
        ]
    ),
    Extension(
        name="opteryx.compiled.functions.functions",
        sources=["opteryx/compiled/functions/functions.pyx"],
        include_dirs=include_dirs,
        extra_compile_args=COMPILE_FLAGS,
    ),
    Extension(
        name="opteryx.compiled.functions.ip_address",
        sources=["opteryx/compiled/functions/ip_address.pyx"],
        include_dirs=include_dirs,
        extra_compile_args=COMPILE_FLAGS,
    ),
    Extension(
        name="opteryx.compiled.functions.murmurhash3_32",
        sources=["opteryx/compiled/functions/murmurhash3_32.pyx"],
        extra_compile_args=COMPILE_FLAGS,
    ),
    Extension(
        name="opteryx.compiled.functions.levenstein",
        sources=["opteryx/compiled/functions/levenshtein.pyx"],
        extra_compile_args=COMPILE_FLAGS,
    ),
    Extension(
        name="opteryx.compiled.functions.vectors",
        sources=["opteryx/compiled/functions/vectors.pyx"],
        include_dirs=include_dirs,
        language="c++",
        extra_compile_args=COMPILE_FLAGS + ["-std=c++17"],
    ),
    Extension(
        name="opteryx.compiled.joins.cross_join",
        sources=[
            "opteryx/compiled/joins/cross_join.pyx"
            ],
        language="c++",
        include_dirs=include_dirs + ["third_party/abseil"],
        extra_compile_args=COMPILE_FLAGS + ["-std=c++17"],
    ),
    Extension(
        name="opteryx.compiled.joins.filter_join",
        sources=[
            "opteryx/compiled/joins/filter_join.pyx",
            ],
        language="c++",
        include_dirs=include_dirs + ["third_party/abseil"],
        extra_compile_args=COMPILE_FLAGS + ["-std=c++17"],
    ),
    Extension(
        name="opteryx.compiled.joins.inner_join",
        sources=[
            "opteryx/compiled/joins/inner_join.pyx",
            ],
        language="c++",
        include_dirs=include_dirs + ["third_party/abseil"],
        extra_compile_args=COMPILE_FLAGS + ["-std=c++17"],
    ),
    Extension(
        name="opteryx.compiled.joins.outer_join",
        sources=[
            "opteryx/compiled/joins/outer_join.pyx",
            ],
        language="c++",
        include_dirs=include_dirs + ["third_party/abseil"],
        extra_compile_args=COMPILE_FLAGS + ["-std=c++17"],
    ),
    Extension(
        name="opteryx.compiled.list_ops.list_ops",
        sources=[
            "opteryx/compiled/list_ops/list_ops.pyx",
        ],
        language="c++",
        include_dirs=include_dirs + ["third_party/abseil"],
        extra_compile_args=COMPILE_FLAGS + ["-std=c++17"],
    ),
    Extension(
        name="opteryx.compiled.structures.hash_table",
        sources=["opteryx/compiled/structures/hash_table.pyx"],
        include_dirs=include_dirs,
        language="c++",
        extra_compile_args=COMPILE_FLAGS + ["-std=c++17"],
    ),
    Extension(
        name="opteryx.compiled.structures.bloom_filter",
        sources=["opteryx/compiled/structures/bloom_filter.pyx"],
        include_dirs=include_dirs,
        extra_compile_args=COMPILE_FLAGS,
    ),
    Extension(
        name="opteryx.compiled.structures.buffers",
        sources=["opteryx/compiled/structures/buffers.pyx"],
        include_dirs=include_dirs, # + [pyarrow.get_include()],
        language="c++",
        extra_compile_args=COMPILE_FLAGS + ["-std=c++17"],
    ),
    Extension(
        name="opteryx.compiled.structures.memory_pool",
        sources=["opteryx/compiled/structures/memory_pool.pyx"],
        language="c++",
        extra_compile_args=COMPILE_FLAGS,
    ),
    Extension(
        name="opteryx.compiled.structures.node",
        sources=["opteryx/compiled/structures/node.pyx"],
        extra_compile_args=COMPILE_FLAGS,
    ),
    Extension(
        name="opteryx.compiled.table_ops.distinct",
        sources=["opteryx/compiled/table_ops/distinct.pyx"],
        include_dirs=include_dirs + ["third_party/abseil"],
        language="c++",
        extra_compile_args=COMPILE_FLAGS + ["-std=c++17"],
    ),
    Extension(
        name="opteryx.third_party.fuzzy",
        sources=["opteryx/compiled/third_party/fuzzy_soundex.pyx"],
        extra_compile_args=COMPILE_FLAGS,
    ),
]


setup_config = {
    "name": LIBRARY,
    "version": __version__,
    "description": "Python SQL Query Engine",
    "long_description": long_description,
    "long_description_content_type": "text/markdown",
    "maintainer": "@joocer",
    "author": __author__,
    "author_email": "justin.joyce@joocer.com",
    "packages": find_packages(include=[LIBRARY, f"{LIBRARY}.*"]),
    "python_requires": ">=3.9",
    "url": "https://github.com/mabel-dev/opteryx/",
    "install_requires": required,
    "ext_modules": cythonize(extensions),
    "entry_points": {
        "console_scripts": ["opteryx=opteryx.command:main"],
    },
    "package_data": {
        "": ["*.pyx", "*.pxd"],
    },
}

rust_build(setup_config)

setup(**setup_config)
