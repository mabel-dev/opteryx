"""
This setup script builds and installs Opteryx by compiling both Cython extensions and a
Rust module.

It detects the operating system to set the correct compiler flags, organizes
include paths, processes dependencies, and reads in version and project metadata. Multiple
extension modules are defined for optimized data operations.
"""

import glob
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


def is_mac():  # pragma: no cover
    return platform.system().lower() == "darwin"

def is_win():  # pragma: no cover
    return platform.system().lower() == "windows"

LIBRARY = "opteryx"
CPP_COMPILE_FLAGS = ["-O3"]
C_COMPILE_FLAGS = ["-O3"]
if is_mac():
    CPP_COMPILE_FLAGS += ["-std=c++17"]
elif is_win():
    CPP_COMPILE_FLAGS += ["/std:c++17"]
else:    
    CPP_COMPILE_FLAGS += ["-std=c++17", "-march=native", "-fvisibility=default"]
    C_COMPILE_FLAGS += ["-march=native", "-fvisibility=default"]

# Dynamically get the default include paths
include_dirs = [numpy.get_include(), "src/cpp", "src/c"]

# Get the C++ include directory
includedir = get_config_var('INCLUDEDIR')
if includedir:
    include_dirs.append(os.path.join(includedir, 'c++', 'v1'))

# Get the Python include directory
includepy = get_config_var('INCLUDEPY')
if includepy:
    include_dirs.append(includepy)

# Check if paths exist
include_dirs = [p for p in include_dirs if os.path.exists(p)]

print("Include paths:", include_dirs)

def rust_build(setup_kwargs: Dict[str, Any]) -> None:
    setup_kwargs.update(
        {
            "rust_extensions": [RustExtension("opteryx.compute", "Cargo.toml", debug=False)],
            "zip_safe": False,
        }
    )


__author__ = "notset"
__version__ = "notset"
_status = None
VersionStatus = None
with open(f"{LIBRARY}/__version__.py", mode="r") as v:
    vers = v.read()
exec(vers)  # nosec

RELEASE_CANDIDATE = _status == VersionStatus.RELEASE
COMPILER_DIRECTIVES = {"language_level": "3"}
COMPILER_DIRECTIVES["profile"] = not RELEASE_CANDIDATE
COMPILER_DIRECTIVES["linetrace"] = not RELEASE_CANDIDATE

print(f"Building Opteryx version: {__version__}")
print(f"Status: {_status}", "(rc)" if RELEASE_CANDIDATE else "")

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
            "third_party/abseil/absl/hash/internal/low_level_hash.cc",
            "third_party/abseil/absl/base/internal/raw_logging.cc",
            "third_party/abseil/absl/base/internal/strerror.cc",
            "third_party/abseil/absl/base/internal/sysinfo.cc",
            "third_party/abseil/absl/base/internal/spinlock_wait.cc",
            ],
        include_dirs=include_dirs + ["third_party/abseil"],
        language="c++",
        extra_compile_args=CPP_COMPILE_FLAGS,
        extra_link_args=["-Lthird_party/abseil"],
    ),
    Extension(
        name="opteryx.third_party.cyan4973.xxhash",
        sources=[
            "opteryx/compiled/third_party/xxhash.pyx",
            "third_party/cyan4973/xxhash.c"
            ],
        include_dirs=include_dirs + ["third_party/cyan4973"],
        extra_compile_args=C_COMPILE_FLAGS,
        extra_link_args=["-Lthird_party/cyan4973"],
    ),
    Extension(
        name='opteryx.third_party.tktech.csimdjson',
        sources=[
            "third_party/tktech/simdjson/simdjson.cpp",
            "third_party/tktech/simdjson/util.cpp",
            "third_party/tktech/simdjson/csimdjson.pyx",
            ],
        language="c++",
        extra_compile_args=CPP_COMPILE_FLAGS
    ),
    Extension(
        name="opteryx.compiled.functions.functions",
        sources=["opteryx/compiled/functions/functions.pyx"],
        include_dirs=include_dirs,
        extra_compile_args=C_COMPILE_FLAGS,
    ),
    Extension(
        name="opteryx.compiled.functions.ip_address",
        sources=["opteryx/compiled/functions/ip_address.pyx"],
        include_dirs=include_dirs,
        extra_compile_args=C_COMPILE_FLAGS,
    ),
    Extension(
        name="opteryx.compiled.functions.levenstein",
        sources=["opteryx/compiled/functions/levenshtein.pyx"],
        extra_compile_args=C_COMPILE_FLAGS,
    ),
    Extension(
        name="opteryx.compiled.functions.vectors",
        sources=["opteryx/compiled/functions/vectors.pyx"],
        include_dirs=include_dirs,
        language="c++",
        extra_compile_args=CPP_COMPILE_FLAGS,
    ),
    Extension(
        name="opteryx.compiled.aggregations.count_distinct",
        sources=["opteryx/compiled/aggregations/count_distinct.pyx"],
        language="c++",
        include_dirs=include_dirs + ["third_party/abseil"],
        extra_compile_args=CPP_COMPILE_FLAGS,
    ),
    Extension(
        name="opteryx.compiled.joins.cross_join",
        sources=["opteryx/compiled/joins/cross_join.pyx"],
        language="c++",
        include_dirs=include_dirs + ["third_party/abseil"],
        extra_compile_args=CPP_COMPILE_FLAGS,
    ),
    Extension(
        name="opteryx.compiled.joins.filter_join",
        sources=["opteryx/compiled/joins/filter_join.pyx"],
        language="c++",
        include_dirs=include_dirs + ["third_party/abseil"],
        extra_compile_args=CPP_COMPILE_FLAGS,
    ),
    Extension(
        name="opteryx.compiled.joins.inner_join",
        sources=["opteryx/compiled/joins/inner_join.pyx"],
        language="c++",
        include_dirs=include_dirs + ["third_party/abseil"],
        extra_compile_args=CPP_COMPILE_FLAGS,
    ),
    Extension(
        name="opteryx.compiled.joins.outer_join",
        sources=["opteryx/compiled/joins/outer_join.pyx"],
        language="c++",
        include_dirs=include_dirs + ["third_party/abseil"],
        extra_compile_args=CPP_COMPILE_FLAGS,
    ),
    Extension(
        name="opteryx.compiled.structures.hash_table",
        sources=["opteryx/compiled/structures/hash_table.pyx"],
        include_dirs=include_dirs,
        language="c++",
        extra_compile_args=CPP_COMPILE_FLAGS,
    ),
    Extension(
        name="opteryx.compiled.structures.bloom_filter",
        sources=["opteryx/compiled/structures/bloom_filter.pyx"],
        include_dirs=include_dirs,
        extra_compile_args=C_COMPILE_FLAGS,
    ),
    Extension(
        name="opteryx.compiled.structures.buffers",
        sources=[
            "opteryx/compiled/structures/buffers.pyx",
            "src/cpp/intbuffer.cpp"
        ],
        include_dirs=include_dirs,
        language="c++",
        define_macros=[("NPY_NO_DEPRECATED_API", "NPY_1_7_API_VERSION")],
        extra_compile_args=CPP_COMPILE_FLAGS + ["-Wall", "-shared"],
    ),
    Extension(
        name="opteryx.compiled.structures.memory_pool",
        sources=["opteryx/compiled/structures/memory_pool.pyx"],
        language="c++",
        extra_compile_args=CPP_COMPILE_FLAGS,
    ),
    Extension(
        name="opteryx.compiled.structures.node",
        sources=["opteryx/compiled/structures/node.pyx"],
        extra_compile_args=C_COMPILE_FLAGS,
    ),
    Extension(
        name="opteryx.compiled.table_ops.distinct",
        sources=["opteryx/compiled/table_ops/distinct.pyx"],
        include_dirs=include_dirs + ["third_party/abseil"],
        language="c++",
        extra_compile_args=CPP_COMPILE_FLAGS,
    ),
    Extension(
        name="opteryx.compiled.table_ops.hash_ops",
        sources=["opteryx/compiled/table_ops/hash_ops.pyx"],
        include_dirs=include_dirs + ["third_party/abseil"],
        language="c++",
        extra_compile_args=CPP_COMPILE_FLAGS,
    ),
    Extension(
        name="opteryx.compiled.table_ops.null_avoidant_ops",
        sources=["opteryx/compiled/table_ops/null_avoidant_ops.pyx"],
        include_dirs=include_dirs,
        language="c++",
        extra_compile_args=CPP_COMPILE_FLAGS,
    ),
    Extension(
        name="opteryx.third_party.fuzzy",
        sources=["opteryx/compiled/third_party/fuzzy_soundex.pyx"],
        extra_compile_args=C_COMPILE_FLAGS,
    ),
]

# Add SIMD support flags
machine = platform.machine().lower()
system = platform.system().lower()
if machine.startswith("arm") and not machine.startswith("aarch64"):
    if system != "darwin":
        CPP_COMPILE_FLAGS.append("-mfpu=neon")
elif "x86" in machine or "amd64" in machine:
    CPP_COMPILE_FLAGS.append("-mavx2")

for cython_file in glob.iglob("opteryx/compiled/list_ops/*.pyx"):
    if is_win():
        cython_file = cython_file.replace("\\", "/")
    module_name = cython_file.replace("/", ".").replace(".pyx", "")
    print(f"Processing file: {cython_file}, module name: {module_name}")
    extensions.append(
            Extension(
                name=module_name,
                sources=[
                    cython_file,
                    "src/cpp/simd_search.cpp"
                    ],
                language="c++",
                include_dirs=include_dirs + ["third_party/abseil"],
                extra_compile_args=CPP_COMPILE_FLAGS,
            ),
    )


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
    "compiler_directives": COMPILER_DIRECTIVES,
}

rust_build(setup_config)

setup(**setup_config)
