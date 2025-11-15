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
import sys
from sysconfig import get_config_var
from typing import Any
from typing import Dict

import numpy
from Cython.Build import cythonize
from setuptools import Extension
from setuptools import find_packages
from setuptools import setup
from setuptools.command.build_ext import build_ext as build_ext_orig
from setuptools_rust import RustExtension

LIBRARY = "opteryx"

class build_ext(build_ext_orig):
    """Ensure the compiler recognizes vendored assembly sources."""

    def build_extensions(self):
        if self.compiler:
            src_exts = self.compiler.src_extensions
            if ".S" not in src_exts:
                src_exts.append(".S")
        super().build_extensions()

def is_mac():  # pragma: no cover
    return platform.system().lower() == "darwin"


def is_win():  # pragma: no cover
    return platform.system().lower() == "windows"


def is_linux():  # pragma: no cover
    return platform.system().lower() == "linux"


def detect_target_machine():
    """Detect the target architecture. Prefer environment variables set by
    cibuildwheel or CI (CIBW_ARCHS, CIBW_ARCH, CIBW_BUILD) and fall back to the
    local platform.machine(). This avoids adding flags for the host arch when
    cross-building inside manylinux containers or emulators.
    """
    # Prefer cibuildwheel environment vars which specify target arches
    for key in ("CIBW_ARCHS", "CIBW_ARCH", "CIBW_BUILD"):
        val = os.environ.get(key)
        if not val:
            continue
        s = val.lower()
        if "aarch64" in s or "arm64" in s:
            return "aarch64"
        if "x86_64" in s or "amd64" in s or "x86" in s:
            return "x86_64"
    # Fallback to the runtime platform machine
    return platform.machine().lower()


def _archflags_request_x86_64() -> bool:
    """Check ARCHFLAGS for any x86_64 slices (e.g. macOS universal builds)."""
    archflags = os.environ.get("ARCHFLAGS", "")
    if not archflags:
        return False

    tokens = archflags.replace(",", " ").split()
    idx = 0
    requested = []
    while idx < len(tokens):
        token = tokens[idx].lower()
        if token == "-arch" and idx + 1 < len(tokens):
            requested.append(tokens[idx + 1].lower())
            idx += 2
            continue
        if token.startswith("-arch="):
            requested.append(token.split("=", 1)[1])
        idx += 1
    return any(t in ("x86_64", "x86-64", "amd64") for t in requested)


def targets_include_x86_64() -> bool:
    """Determine if the build should include x86_64-specific sources."""
    if _archflags_request_x86_64():
        return True
    machine = detect_target_machine()
    return machine in ("x86_64", "amd64")


REQUESTED_COMMANDS = {arg.lower() for arg in sys.argv[1:] if arg and not arg.startswith("-")}
SHOULD_BUILD_EXTENSIONS = "clean" not in REQUESTED_COMMANDS
RUGO_PARQUET = "third_party/mabel/rugo/parquet"
RUGO_JSONL = "third_party/mabel/rugo/jsonl"

def validate_draken_package_data():
    """
    Validate that all necessary draken files for wheel distribution exist.
    
    This ensures that .pxd, .h, and .cpp files needed for downstream packages
    to compile Cython code that imports from draken are present.
    
    Raises SystemExit if critical files are missing.
    """
    critical_files = {
        "third_party/mabel/draken/core/buffers.h": "Core buffer type definitions",
        "opteryx/draken/core/buffers.pxd": "Cython declarations for buffers",
        "third_party/mabel/draken/core/ops.h": "Core operations header",
        "opteryx/draken/vectors/string_vector.pxd": "String vector declarations",
        "third_party/mabel/draken/interop/arrow_c_data_interface.h": "Arrow C interface",
    }
    
    missing_files = []
    for filepath, description in critical_files.items():
        if not os.path.exists(filepath):
            missing_files.append(f"  - {filepath} ({description})")
    
    if missing_files:
        print("\033[91m✗ CRITICAL: Missing draken package files:\033[0m")
        for missing in missing_files:
            print(missing)
        print("\nThese files are required for the wheel to be usable by downstream packages.")
        sys.exit(1)
    else:
        print("\033[92m✓ Draken package data validation passed\033[0m")

def get_parquet_vendor_sources():
    """Get vendored compression library sources"""
    vendor_sources = []
    
    # Snappy sources (minimal set for decompression only) - these are C++
    snappy_sources = [
        f"{RUGO_PARQUET}/vendor/snappy/snappy.cc",
        f"{RUGO_PARQUET}/vendor/snappy/snappy-sinksource.cc", 
        f"{RUGO_PARQUET}/vendor/snappy/snappy-stubs-internal.cc"
    ]
    vendor_sources.extend(snappy_sources)
    
    # Zstd sources (decompression modules only) - compiled as C++
    zstd_sources = [
        # Common modules
        f"{RUGO_PARQUET}/vendor/zstd/common/entropy_common.cpp",
        f"{RUGO_PARQUET}/vendor/zstd/common/fse_decompress.cpp",
        f"{RUGO_PARQUET}/vendor/zstd/common/zstd_common.cpp",
        f"{RUGO_PARQUET}/vendor/zstd/common/xxhash.cpp",
        f"{RUGO_PARQUET}/vendor/zstd/common/error_private.cpp",
        f"{RUGO_PARQUET}/vendor/zstd/decompress/zstd_decompress.cpp",
        f"{RUGO_PARQUET}/vendor/zstd/decompress/zstd_decompress_block.cpp",
        f"{RUGO_PARQUET}/vendor/zstd/decompress/huf_decompress.cpp",
        f"{RUGO_PARQUET}/vendor/zstd/decompress/zstd_ddict.cpp"
    ]

    machine_ = detect_target_machine()
    if targets_include_x86_64():
        # BMI2-enabled builds expect this ASM fast path to be present on x86-64.
        zstd_sources.append(f"{RUGO_PARQUET}/vendor/zstd/decompress/huf_decompress_amd64.S")

    vendor_sources.extend(zstd_sources)

    return vendor_sources



if not SHOULD_BUILD_EXTENSIONS:
    REQUESTED_DISPLAY = ", ".join(sorted(REQUESTED_COMMANDS)) or "<none>"
    print(
        f"\033[38;2;255;208;0mSkipping native extension build for command(s):\033[0m {REQUESTED_DISPLAY}"
    )

if SHOULD_BUILD_EXTENSIONS:
    CPP_COMPILE_FLAGS = ["-O3"]
    C_COMPILE_FLAGS = ["-O3"]
    if is_mac():
        CPP_COMPILE_FLAGS += ["-std=c++17"]
        C_COMPILE_FLAGS += ["-std=c17"]
    elif is_win():
        CPP_COMPILE_FLAGS += ["/std:c++17"]
        C_COMPILE_FLAGS += ["/std:c17"]
    else:
        CPP_COMPILE_FLAGS += ["-std=c++17", "-march=native", "-fvisibility=default"]
        C_COMPILE_FLAGS += ["-std=c17", "-march=native", "-fvisibility=default"]

    COMMON_WARNING_SUPPRESSIONS = [
        "-Wno-unused-function",
        "-Wno-unreachable-code-fallthrough",
        "-Wno-sign-compare",
        "-Wno-integer-overflow",
        "-Wno-unused-command-line-argument",
    ]
    C_COMPILE_FLAGS.extend(COMMON_WARNING_SUPPRESSIONS)
    CPP_COMPILE_FLAGS.extend(COMMON_WARNING_SUPPRESSIONS)

    # JSON lines reader extension with SIMD optimizations
    JSONL_COMPILE_FLAGS = CPP_COMPILE_FLAGS.copy()
    # Add SIMD flags based on architecture
    machine_ = detect_target_machine()
    if machine_ in ("x86_64", "amd64"):
        # x86-64: Add SSE4.2 and AVX2 flags
        CPP_COMPILE_FLAGS.extend(["-msse4.2", "-mavx2"])


    # Dynamically get the default include paths
    include_dirs = [numpy.get_include(), "src/cpp", "src/c", "third_party/mabel/draken"]

    # Get the C++ include directory
    includedir = get_config_var("INCLUDEDIR")
    if includedir:
        include_dirs.append(os.path.join(includedir, "c++", "v1"))

    # Get the Python include directory
    includepy = get_config_var("INCLUDEPY")
    if includepy:
        include_dirs.append(includepy)

    # Check if paths exist
    include_dirs = [p for p in include_dirs if os.path.exists(p)]

    print("\033[38;2;255;85;85mInclude paths:\033[0m", include_dirs)

    def rust_build(setup_kwargs: Dict[str, Any]) -> None:
        setup_kwargs.update(
            {
                "rust_extensions": [RustExtension("opteryx.compute", "Cargo.toml", debug=False)],
                "zip_safe": False,
            }
        )

    __author__ = "notset"
    __version__ = "notset"

    with open(f"{LIBRARY}/__version__.py", mode="r", encoding="UTF8") as v:
        vers = v.read()
    exec(vers)  # nosec

    RELEASE_CANDIDATE = "a" not in __version__ and "b" not in __version__
    COMPILER_DIRECTIVES = {"language_level": "3"}
    COMPILER_DIRECTIVES["profile"] = not RELEASE_CANDIDATE
    COMPILER_DIRECTIVES["linetrace"] = not RELEASE_CANDIDATE
    
    # Enable line tracing for profiling in non-release builds
    if RELEASE_CANDIDATE:
        CPP_COMPILE_FLAGS.append("-DCYTHON_TRACE=1")
        CPP_COMPILE_FLAGS.append("-DCYTHON_TRACE_NOGIL=1")
        C_COMPILE_FLAGS.append("-DCYTHON_TRACE=1")
        C_COMPILE_FLAGS.append("-DCYTHON_TRACE_NOGIL=1")

    print(f"\033[38;2;255;85;85mBuilding Opteryx version:\033[0m {__version__}")
    print(f"\033[38;2;255;85;85mStatus:\033[0m (test)", "(rc)" if RELEASE_CANDIDATE else "")

    with open("README.md", mode="r", encoding="UTF8") as rm:
        long_description = rm.read()

    def make_draken_extension(module_path, source_file, depends=None, language=None, **kwargs):
        """
        Helper function to create draken extensions with consistent configuration.
        
        Args:
            module_path: Module path relative to opteryx.draken (e.g., "vectors.int64_vector")
            source_file: Source file path relative to third_party/mabel/draken/ (can be string or list)
            depends: List of header files this extension depends on
            language: "c++" for C++ extensions, None for C
            **kwargs: Additional arguments passed to Extension
        
        Returns:
            Extension object
        """
        if depends is None:
            depends = ["third_party/mabel/draken/core/buffers.h"]
        
        # Handle both string and list of source files
        if isinstance(source_file, str):
            sources = [f"third_party/mabel/draken/{source_file}"]
        else:
            sources = [f"third_party/mabel/draken/{sf}" for sf in source_file]
        
        ext_kwargs = {
            "name": f"opteryx.draken.{module_path}",
            "sources": sources,
            "extra_compile_args": CPP_COMPILE_FLAGS if language == "c++" else C_COMPILE_FLAGS,
            "include_dirs": include_dirs + ["third_party/mabel/draken"],
            "depends": depends,
        }
        
        if language:
            ext_kwargs["language"] = language
        
        ext_kwargs.update(kwargs)
        return Extension(**ext_kwargs)

    extensions = [
        Extension(
            name="opteryx.third_party.abseil.containers",
            sources=[
                "opteryx/third_party/abseil/containers.pyx",
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
            name="opteryx.third_party.alantsd.base64",
            sources=["opteryx/third_party/alantsd/base64.pyx", "third_party/alantsd/base64.c"],
            include_dirs=include_dirs + ["third_party/alantsd"],
            extra_compile_args=C_COMPILE_FLAGS + ["-std=c99", "-DBASE64_IMPLEMENTATION"],
            extra_link_args=["-Lthird_party/alantsd"],
        ),
        Extension(
            name="opteryx.third_party.cyan4973.xxhash",
            sources=[
                "opteryx/third_party/cyan4973/xxhash.pyx",
                "third_party/cyan4973/xxhash.c",
            ],
            include_dirs=include_dirs + ["third_party/cyan4973"],
            extra_compile_args=C_COMPILE_FLAGS,
            extra_link_args=["-Lthird_party/cyan4973"],
        ),
        Extension(
            name="opteryx.third_party.fastfloat.fast_float",
            sources=[
                "opteryx/third_party/fastfloat/fast_float.pyx",
            ],
            include_dirs=include_dirs + ["third_party/fastfloat/fast_float"],
            language="c++",
            extra_compile_args=CPP_COMPILE_FLAGS,
            extra_link_args=["-Lthird_party/fastfloat/fast_float"],
        ),
        Extension(
            name="opteryx.third_party.tktech.csimdjson",
            sources=[
                "opteryx/third_party/tktech/csimdjson.pyx",
                "third_party/tktech/simdjson/simdjson.cpp",
                "third_party/tktech/simdjson/util.cpp",
            ],
            include_dirs=include_dirs + ["third_party/tktech/simdjson"],
            language="c++",
            extra_compile_args=CPP_COMPILE_FLAGS,
        ),
        Extension(
            name="opteryx.third_party.ulfjack.ryu",
            sources=[
                "opteryx/third_party/ulfjack/ryu.pyx",
                "third_party/ulfjack/ryu/d2fixed.c",
            ],
            include_dirs=include_dirs + ["third_party/ulfjack/ryu"],
            extra_compile_args=C_COMPILE_FLAGS,
            extra_link_args=["-Lthird_party/ulfjack/ryu"],
        ),
        Extension(
                "opteryx.rugo.parquet",
                sources=[
                    f"{RUGO_PARQUET}/parquet_reader.pyx",
                    f"{RUGO_PARQUET}/metadata.cpp",
                    f"{RUGO_PARQUET}/bloom_filter.cpp",
                    f"{RUGO_PARQUET}/decode.cpp",
                    f"{RUGO_PARQUET}/compression.cpp",
                ] + get_parquet_vendor_sources(),  # ADD: vendored compression libraries
                include_dirs=[
                    f"{RUGO_PARQUET}/vendor/snappy",      # Snappy headers
                    f"{RUGO_PARQUET}/vendor/zstd",        # Zstd main header
                    f"{RUGO_PARQUET}/vendor/zstd/common", # Zstd common headers
                    f"{RUGO_PARQUET}/vendor/zstd/decompress" # Zstd decompress headers
                ],
                define_macros=[
                    ("HAVE_SNAPPY", "1"),
                    ("HAVE_ZSTD", "1"),
                    ("ZSTD_STATIC_LINKING_ONLY", "1")  # Enable zstd static linking
                ],
                language="c++",
                extra_compile_args=CPP_COMPILE_FLAGS,
                extra_link_args=[],
        ),
        Extension(
                "opteryx.rugo.jsonl",
                sources=[
                    f"{RUGO_JSONL}/jsonl_reader.pyx",
                    f"{RUGO_JSONL}/decode.cpp",
                    f"{RUGO_JSONL}/simdjson_wrapper.cpp",
                    "third_party/tktech/simdjson/simdjson.cpp",  # Consolidated simdjson
                ],
                include_dirs=[
                    f"{RUGO_JSONL}",
                    "third_party/tktech/simdjson",     # Consolidated simdjson
                    "third_party/fastfloat",           # Consolidated fast_float
                ] + include_dirs,
                language="c++",
                extra_compile_args=JSONL_COMPILE_FLAGS,
                extra_link_args=[],
        ),




        # Draken extensions (vendored columnar data library)
        make_draken_extension(
            "interop.arrow",
            "interop/arrow.pyx",
            depends=[
                "third_party/mabel/draken/core/buffers.h",
                "third_party/mabel/draken/interop/arrow_c_data_interface.h"
            ],
        ),
        make_draken_extension("vectors.vector", "vectors/vector.pyx"),
        make_draken_extension(
            "core.ops",
            ["core/ops.pyx", "core/ops_impl.cpp"],
            depends=[
                "third_party/mabel/draken/core/buffers.h",
                "third_party/mabel/draken/core/ops.h"
            ],
            language="c++",
        ),
        make_draken_extension("vectors.bool_vector", "vectors/bool_vector.pyx"),
        make_draken_extension("vectors.float64_vector", "vectors/float64_vector.pyx"),
        make_draken_extension("vectors.int64_vector", "vectors/int64_vector.pyx", language="c++"),
        make_draken_extension("vectors.string_vector", "vectors/string_vector.pyx"),
        make_draken_extension("vectors.date32_vector", "vectors/date32_vector.pyx"),
        make_draken_extension("vectors.timestamp_vector", "vectors/timestamp_vector.pyx"),
        make_draken_extension("vectors.time_vector", "vectors/time_vector.pyx"),
        make_draken_extension("vectors.interval_vector", "vectors/interval_vector.pyx"),
        make_draken_extension("vectors.array_vector", "vectors/array_vector.pyx"),
        Extension(
            name="opteryx.draken.vectors._hash_api",
            sources=["opteryx/draken/vectors/_hash_api.pyx"],
            include_dirs=include_dirs + ["third_party/mabel/draken"],
            extra_compile_args=C_COMPILE_FLAGS,
        ),
        make_draken_extension(
            "morsels.morsel",
            "morsels/morsel.pyx",
            depends=[
                "third_party/mabel/draken/core/buffers.h",
                "third_party/mabel/draken/morsels/morsel.h"
            ],
        ),
        make_draken_extension(
            "morsels.align",
            "morsels/align.pyx",
            depends=[
                "third_party/mabel/draken/core/buffers.h",
                "third_party/mabel/draken/morsels/morsel.pxd"
            ],
        ),

        Extension(
            name="opteryx.compiled.functions.strings",
            sources=["opteryx/compiled/functions/strings.pyx", "src/cpp/simd_search.cpp"],
            include_dirs=include_dirs,
            language="c++",
            extra_compile_args=CPP_COMPILE_FLAGS,
        ),
        Extension(
            name="opteryx.compiled.functions.timestamp",
            sources=["opteryx/compiled/functions/timestamp.pyx"],
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
            sources=["opteryx/compiled/structures/buffers.pyx", "src/cpp/intbuffer.cpp"],
            include_dirs=include_dirs,
            language="c++",
            define_macros=[("NPY_NO_DEPRECATED_API", "NPY_1_7_API_VERSION")],
            extra_compile_args=CPP_COMPILE_FLAGS + ["-Wall", "-shared"],
        ),
        Extension(
            name="opteryx.compiled.structures.lru_k",
            sources=["opteryx/compiled/structures/lru_k.pyx"],
            language="c++",
            extra_compile_args=CPP_COMPILE_FLAGS,
        ),
        Extension(
            name="opteryx.compiled.structures.memory_pool",
            sources=["opteryx/compiled/structures/memory_pool.pyx"],
            language="c++",
            extra_compile_args=CPP_COMPILE_FLAGS,
        ),
        Extension(
            name="opteryx.compiled.structures.relation_statistics",
            sources=["opteryx/compiled/structures/relation_statistics.pyx"],
            language="c++",
            extra_compile_args=CPP_COMPILE_FLAGS,
        ),
        Extension(
            name="opteryx.compiled.structures.node",
            sources=["opteryx/compiled/structures/node.pyx"],
            extra_compile_args=C_COMPILE_FLAGS,
        ),
        Extension(
            name="opteryx.compiled.io.disk_reader",
            sources=[
                "opteryx/compiled/io/disk_reader.pyx",
                "src/cpp/disk_io.cpp",
                "src/cpp/directories.cpp",
            ],
            include_dirs=include_dirs + ["src/cpp"],
            language="c++",
            extra_compile_args=CPP_COMPILE_FLAGS,
            depends=["src/cpp/directories.h"],
        ),
        Extension(
            name="opteryx.compiled.table_ops.distinct",
            sources=["opteryx/compiled/table_ops/distinct.pyx", "src/cpp/intbuffer.cpp"],
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
            sources=["opteryx/third_party/fuzzy/soundex.pyx"],
            extra_compile_args=C_COMPILE_FLAGS,
        ),
        Extension(
            name="opteryx.compiled.structures.memory_view_stream",
            sources=["opteryx/compiled/structures/memory_view_stream.pyx"],
            include_dirs=include_dirs,
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

    # Auto-generate list_ops.pyx to include all individual .pyx files in the folder
    # This ensures new files are automatically included when added
    list_ops_dir = "opteryx/compiled/list_ops"
    list_ops_file = os.path.join(list_ops_dir, "list_ops.pyx")

    # Find all .pyx files in the list_ops directory (excluding list_ops.pyx itself)
    pyx_files = sorted(
        [
            os.path.basename(f)
            for f in glob.glob(os.path.join(list_ops_dir, "*.pyx"))
            if os.path.basename(f) != "list_ops.pyx"
        ]
    )

    # Generate the list_ops.pyx file with include directives
    with open(list_ops_file, "w", encoding="UTF8") as f:
        f.write("""# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False

\"\"\"
Auto-generated consolidated list_ops module.
This file is automatically generated by setup.py and includes all individual
list operation files in the list_ops directory.

DO NOT EDIT THIS FILE MANUALLY - it will be overwritten during build.
\"\"\"

""")

        # Add include directives for each .pyx file
        for pyx_file in pyx_files:
            f.write(f'include "{pyx_file}"\n')

    print(
        f"\033[38;2;189;147;249mAuto-generated list_ops.pyx with {len(pyx_files)} includes\033[0m"
    )

    # Auto-generate joins.pyx to include all individual .pyx files in the folder
    # This ensures new files are automatically included when added
    joins_dir = "opteryx/compiled/joins"
    joins_file = os.path.join(joins_dir, "joins.pyx")

    # Find all .pyx files in the joins directory (excluding joins.pyx itself)
    joins_pyx_files = sorted(
        [
            os.path.basename(f)
            for f in glob.glob(os.path.join(joins_dir, "*.pyx"))
            if os.path.basename(f) != "joins.pyx"
        ]
    )

    # Generate the joins.pyx file with include directives
    with open(joins_file, "w", encoding="UTF8") as f:
        f.write("""# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False

\"\"\"
Auto-generated consolidated joins module.
This file is automatically generated by setup.py and includes all individual
join files in the joins directory.

DO NOT EDIT THIS FILE MANUALLY - it will be overwritten during build.
\"\"\"

""")

        # Add include directives for each .pyx file
        for pyx_file in joins_pyx_files:
            f.write(f'include "{pyx_file}"\n')

    print(
        f"\033[38;2;189;147;249mAuto-generated joins.pyx with {len(joins_pyx_files)} includes\033[0m"
    )

    re2_sources = [
        "third_party/re2/re2/bitstate.cc",
        "third_party/re2/re2/compile.cc",
        "third_party/re2/re2/dfa.cc",
        "third_party/re2/re2/filtered_re2.cc",
        "third_party/re2/re2/mimics_pcre.cc",
        "third_party/re2/re2/nfa.cc",
        "third_party/re2/re2/onepass.cc",
        "third_party/re2/re2/parse.cc",
        "third_party/re2/re2/perl_groups.cc",
        "third_party/re2/re2/prefilter.cc",
        "third_party/re2/re2/prefilter_tree.cc",
        "third_party/re2/re2/prog.cc",
        "third_party/re2/re2/re2.cc",
        "third_party/re2/re2/regexp.cc",
        "third_party/re2/re2/set.cc",
        "third_party/re2/re2/simplify.cc",
        "third_party/re2/re2/stringpiece.cc",
        "third_party/re2/re2/tostring.cc",
        "third_party/re2/re2/unicode_casefold.cc",
        "third_party/re2/re2/unicode_groups.cc",
        "third_party/re2/util/rune.cc",
        "third_party/re2/util/strutil.cc",
    ]

    list_ops_link_args = []
    if not is_mac():
        list_ops_link_args.append("-lcrypto")
    if not is_win():
        list_ops_link_args.append("-pthread")

    for ext in extensions:
        if ext.name.startswith("opteryx.draken.vectors."):
            if "src/cpp/simd_hash.cpp" not in ext.sources:
                ext.sources.append("src/cpp/simd_hash.cpp")
            ext.language = "c++"
            ext.extra_compile_args = CPP_COMPILE_FLAGS

    extensions.append(
        Extension(
            name="opteryx.compiled.list_ops.function_definitions",
            sources=[list_ops_file, "src/cpp/simd_search.cpp"] + re2_sources,
            language="c++",
            include_dirs=include_dirs
            + [
                "third_party/abseil",
                "third_party/apache",
                "opteryx/third_party/apache",
                "third_party/re2",
                "third_party/mabel/draken",
            ],
            extra_compile_args=CPP_COMPILE_FLAGS,
            extra_link_args=list_ops_link_args,
        ),
    )

    extensions.append(
        Extension(
            name="opteryx.compiled.joins.join_definitions",
            sources=[joins_file, "src/cpp/join_kernels.cpp", "src/cpp/intbuffer.cpp"],
            language="c++",
            include_dirs=include_dirs + ["third_party/abseil", "third_party/fastfloat/fast_float"],
            extra_compile_args=CPP_COMPILE_FLAGS,
        ),
    )

    setup_config = {
        "name": LIBRARY,
        "version": __version__,
        "description": "Python SQL Query Engine",
        "long_description": long_description,
        "long_description_content_type": "text/markdown",
        "packages": find_packages(include=[LIBRARY, f"{LIBRARY}.*"]),
        "python_requires": ">=3.11",
        "url": "https://github.com/mabel-dev/opteryx/",
        "ext_modules": (
            cythonize(extensions, compiler_directives=COMPILER_DIRECTIVES)
            if SHOULD_BUILD_EXTENSIONS
            else []
        ),
        "entry_points": {
            "console_scripts": ["opteryx=opteryx.command:main"],
        },
        "package_data": {
            "": ["*.pyx", "*.pxd", "*.h"],
            "opteryx": ["third_party/**/*.h"],
        },
        "cmdclass": {"build_ext": build_ext},
    }

    rust_build(setup_config)

    # Validate package data for draken vendoring
    validate_draken_package_data()

    setup(**setup_config)
