"""
Simplified setup script for Opteryx - builds all Cython extensions and Rust module.
"""

import glob
import os
import platform
import sys

import numpy
from Cython.Build import cythonize
from setuptools import Extension
from setuptools import find_packages
from setuptools import setup
from setuptools.command.build_ext import build_ext as build_ext_orig
from setuptools_rust import RustExtension

LIBRARY = "opteryx"

class build_ext(build_ext_orig):
    def build_extensions(self):
        if self.compiler and ".S" not in self.compiler.src_extensions:
            self.compiler.src_extensions.append(".S")
        super().build_extensions()

# Platform detection
def is_mac(): return platform.system() == "Darwin"
def is_win(): return platform.system() == "Windows" 
def is_linux(): return platform.system() == "Linux"

# Skip extension building for clean command
if "clean" in [arg.lower() for arg in sys.argv[1:] if arg and not arg.startswith("-")]:
    print("Skipping native extension build for clean command")
    sys.exit(0)

# Architecture detection for SIMD
def detect_architecture():
    machine = platform.machine().lower()
    # Respect environment ARCHFLAGS when cross-compiling or building universal
    # wheels (e.g., ``-arch x86_64 -arch arm64``). This makes the build
    # deterministic wrt which arch-specific sources to include.
    archflags = os.environ.get("ARCHFLAGS", "").lower()
    has_x86 = "x86_64" in archflags
    has_arm = "arm64" in archflags or "aarch64" in archflags
    if has_x86 and not has_arm:
        return "x86_64"
    if has_arm and not has_x86:
        return "aarch64"
    if has_x86 and has_arm:
        # Universal build - don't infer a single architecture. Leave
        # autodetection using the host platform below, which avoids
        # including arch-specific assembly for both targets in a single
        # compile invocation.
        pass
    # Distinguish between 32-bit ARM (arm/armv7) and 64-bit ARM (aarch64/arm64)
    if "aarch64" in machine or "arm64" in machine:
        return "aarch64"
    if "arm" in machine:
        return "arm"
    if "x86" in machine or "amd64" in machine:
        return "x86_64"
    return machine

# Compiler flags with SIMD support
arch = detect_architecture()
CPP_FLAGS = ["-O3", "-std=c++17"]
C_FLAGS = ["-O3"]

if is_win():
    CPP_FLAGS = ["/O2", "/std:c++17"]
    C_FLAGS = ["/O2"]
elif is_linux():
    CPP_FLAGS.extend(["-march=native", "-fvisibility=default"])
    C_FLAGS.extend(["-march=native", "-fvisibility=default"])

# SIMD-specific flags
if arch == "x86_64":
    # Add SIMD support
    CPP_FLAGS.extend(["-msse4.2", "-mavx2"])
elif arch == "arm" and not is_mac():
    CPP_FLAGS.append("-mfpu=neon")

# Common warning suppressions
WARNING_FLAGS = [
    "-Wno-unused-function",
    "-Wno-unreachable-code-fallthrough", 
    "-Wno-sign-compare",
    "-Wno-unused-command-line-argument",
]
CPP_FLAGS.extend(WARNING_FLAGS)
C_FLAGS.extend(WARNING_FLAGS)

# Include directories
include_dirs = [
    numpy.get_include(),
    "src/cpp", "src/c", 
    "third_party/mabel/draken",
    "third_party/abseil",
    "third_party/fastfloat",
    "third_party/fastfloat/fast_float",
    "third_party/mabel/rugo/parquet",
    "third_party/tktech/simdjson",
    "third_party/re2",
    "third_party/cyan4973",
    "third_party/ulfjack/ryu",
    "third_party/alantsd",
]

# Common SIMD / environment C++ sources used by multiple extensions
COMMON_SIMD_SOURCES = [
    "src/cpp/simd_env.cpp",
    "src/cpp/cpu_features.cpp",
    "src/cpp/simd_search.cpp",
]

# Read version and metadata
with open(f"{LIBRARY}/__version__.py", "r", encoding="UTF8") as v:
    exec(v.read())

with open("README.md", "r", encoding="UTF8") as f:
    long_description = f.read()

# Helper for draken extensions
def make_draken_extension(module_path, source_file, language="c++", depends=None):
    if depends is None:
        depends = ["third_party/mabel/draken/core/buffers.h"]

    sources = [f"third_party/mabel/draken/{source_file}"]
    # Include SIMD hash implementation for all draken vector modules so
    # simd_mix_hash and related functions are available at link time.
    if "src/cpp/simd_hash.cpp" not in sources:
        sources.append("src/cpp/simd_hash.cpp")

    # Common SIMD/environment sources - CPU features and SIMDs
    for s in ("src/cpp/simd_env.cpp", "src/cpp/cpu_features.cpp", "src/cpp/simd_search.cpp"):
        if s not in sources:
            sources.append(s)

    return Extension(
        name=f"opteryx.draken.{module_path}",
        sources=sources,
        include_dirs=include_dirs,
        extra_compile_args=CPP_FLAGS if language == "c++" else C_FLAGS,
        language=language,
        depends=depends,
    )


def get_zstd_vendor_sources():
    """Return the vendored zstd sources so other extensions can link to the same files."""
    RUGO_PARQUET = "third_party/mabel/rugo/parquet"
    sources = [
        f"{RUGO_PARQUET}/vendor/zstd/common/entropy_common.cpp",
        f"{RUGO_PARQUET}/vendor/zstd/common/fse_decompress.cpp",
        f"{RUGO_PARQUET}/vendor/zstd/common/zstd_common.cpp",
        f"{RUGO_PARQUET}/vendor/zstd/common/xxhash.cpp",
        f"{RUGO_PARQUET}/vendor/zstd/common/error_private.cpp",
        f"{RUGO_PARQUET}/vendor/zstd/decompress/zstd_decompress.cpp",
        f"{RUGO_PARQUET}/vendor/zstd/decompress/zstd_decompress_block.cpp",
        f"{RUGO_PARQUET}/vendor/zstd/decompress/huf_decompress.cpp",
        f"{RUGO_PARQUET}/vendor/zstd/decompress/zstd_ddict.cpp",
    ]
    machine = detect_architecture()
    if machine in ("x86_64", "amd64"):
        sources.append(f"{RUGO_PARQUET}/vendor/zstd/decompress/huf_decompress_amd64.S")
    return sources

def get_parquet_vendor_sources():
    """Return vendored zstd/snappy source files to build into parquet extension.

    We only compile the decompression bits (zstd) and minimal snappy sources we
    need for decompression. The vendor code is included inside the project, so
    building them into the extension avoids linking to system libraries and
    avoids runtime missing symbol errors.
    """
    vendor_sources = []
    RUGO_PARQUET = "third_party/mabel/rugo/parquet"

    # Snappy sources (minimal subset for decompress)
    snappy_sources = [
        f"{RUGO_PARQUET}/vendor/snappy/snappy.cc",
        f"{RUGO_PARQUET}/vendor/snappy/snappy-sinksource.cc",
        f"{RUGO_PARQUET}/vendor/snappy/snappy-stubs-internal.cc",
    ]
    vendor_sources.extend(snappy_sources)

    # Zstd decompression sources
    vendor_sources.extend(get_zstd_vendor_sources())
    return vendor_sources

# Link args for parquet extension - ensure libcrypto is linked on Linux so
# the runtime 'ldd' check in CI can verify its presence. Don't add -lcrypto
# on macOS where the system library naming differs.
parquet_link_args = []
if not is_mac():
    # Ensure libcrypto is added to the DT_NEEDED entries of the shared
    # object even if no symbols are referenced (CI asserts its presence).
    parquet_link_args.extend(["-Wl,--no-as-needed", "-lcrypto", "-Wl,--as-needed"])

# Define all extensions
extensions = [
    
    # Third-party libraries
    Extension(
        "opteryx.third_party.abseil.containers",
        sources=[
            "opteryx/third_party/abseil/containers.pyx",
            "third_party/abseil/absl/hash/internal/hash.cc",
            "third_party/abseil/absl/hash/internal/city.cc",
            "third_party/abseil/absl/container/internal/raw_hash_set.cc",
            "third_party/abseil/absl/hash/internal/low_level_hash.cc",
            "third_party/abseil/absl/base/internal/raw_logging.cc",
        ],
        include_dirs=include_dirs,
        language="c++",
        extra_compile_args=CPP_FLAGS,
    ),
    Extension(
        "opteryx.third_party.alantsd.base64",
        sources=[
            "opteryx/third_party/alantsd/base64.pyx",
            "third_party/alantsd/base64.c",
            "third_party/alantsd/base64_dispatch.c",
            "third_party/alantsd/base64_neon.c",
            "third_party/alantsd/base64_avx2.c",
            "third_party/alantsd/base64_avx512.c",
        ],
        include_dirs=include_dirs,
        extra_compile_args=C_FLAGS + ["-std=c99", "-DBASE64_IMPLEMENTATION"],
    ),
    Extension(
        "opteryx.third_party.cyan4973.xxhash",
        sources=["opteryx/third_party/cyan4973/xxhash.pyx", "third_party/cyan4973/xxhash.c"],
        include_dirs=include_dirs,
        extra_compile_args=C_FLAGS,
    ),
    Extension(
        "opteryx.third_party.fastfloat.fast_float", 
        sources=["opteryx/third_party/fastfloat/fast_float.pyx"],
        include_dirs=include_dirs,
        language="c++",
        extra_compile_args=CPP_FLAGS,
    ),
    Extension(
        "opteryx.third_party.tktech.csimdjson",
        sources=[
            "opteryx/third_party/tktech/csimdjson.pyx",
            "third_party/tktech/simdjson/simdjson.cpp",
            "src/cpp/simdjson_error_shim.cpp",
        ],
        include_dirs=include_dirs,
        language="c++",
        extra_compile_args=CPP_FLAGS,
    ),
    Extension(
        "opteryx.third_party.facebook.zstd",
        sources=["opteryx/third_party/facebook/zstd.pyx"] + get_zstd_vendor_sources(),
        include_dirs=
            include_dirs
            + [
                "third_party/mabel/rugo/parquet/vendor/zstd",
                "third_party/mabel/rugo/parquet/vendor/zstd/common",
                "third_party/mabel/rugo/parquet/vendor/zstd/decompress",
            ],
        define_macros=[("ZSTD_STATIC_LINKING_ONLY", "1")],
        language="c++",
        extra_compile_args=CPP_FLAGS,
    ),
    Extension(
        "opteryx.third_party.ulfjack.ryu",
        sources=["opteryx/third_party/ulfjack/ryu.pyx", "third_party/ulfjack/ryu/d2fixed.c"],
        include_dirs=include_dirs,
        extra_compile_args=C_FLAGS,
    ),
    Extension(
        name="opteryx.third_party.fuzzy",
        sources=["opteryx/third_party/fuzzy/soundex.pyx"],
        extra_compile_args=C_FLAGS,
    ),
    
    # File format readers

    Extension(
        "opteryx.rugo.parquet",
        sources=(
            [
                "third_party/mabel/rugo/parquet/parquet_reader.pyx",
                "third_party/mabel/rugo/parquet/metadata.cpp",
                "third_party/mabel/rugo/parquet/decode.cpp",
                "third_party/mabel/rugo/parquet/compression.cpp",
                "third_party/mabel/rugo/parquet/bloom_filter.cpp",
            ] + get_parquet_vendor_sources()
        ),
        include_dirs=(
            include_dirs
            + [
                "third_party/mabel/rugo/parquet/vendor/snappy",
                "third_party/mabel/rugo/parquet/vendor/zstd",
                "third_party/mabel/rugo/parquet/vendor/zstd/common",
                "third_party/mabel/rugo/parquet/vendor/zstd/decompress",
            ]
        ),
        define_macros=[("HAVE_SNAPPY", "1"), ("HAVE_ZSTD", "1"), ("ZSTD_STATIC_LINKING_ONLY", "1")],
        language="c++",
        extra_compile_args=CPP_FLAGS,
        extra_link_args=parquet_link_args,
    ),
    Extension(
        "opteryx.rugo.jsonl", 
        sources=[
            "third_party/mabel/rugo/jsonl/jsonl_reader.pyx",
            "third_party/mabel/rugo/jsonl/decode.cpp",
            "third_party/mabel/rugo/jsonl/simdjson_wrapper.cpp",
            "third_party/tktech/simdjson/simdjson.cpp",
        ],
        include_dirs=include_dirs,
        language="c++",
        extra_compile_args=CPP_FLAGS,
    ),
    
    # Draken core components
    make_draken_extension("interop.arrow", "interop/arrow.pyx"),
    make_draken_extension("vectors.vector", "vectors/vector.pyx"),
    make_draken_extension("vectors.bool_vector", "vectors/bool_vector.pyx"),
    make_draken_extension("vectors.float64_vector", "vectors/float64_vector.pyx"),
    make_draken_extension("vectors.array_vector", "vectors/array_vector.pyx"),
    make_draken_extension("vectors.time_vector", "vectors/time_vector.pyx"),
    make_draken_extension("vectors.interval_vector", "vectors/interval_vector.pyx"),
    make_draken_extension("vectors.int64_vector", "vectors/int64_vector.pyx", language="c++"),
    Extension(
        "opteryx.draken.vectors.string_vector",
        sources=[
            "third_party/mabel/draken/vectors/string_vector.pyx",
            "src/cpp/simd_hash.cpp",
            "src/cpp/cpu_features.cpp",
        ],
        include_dirs=include_dirs,
        define_macros=[("XXH_INLINE_ALL", "1")],
        extra_compile_args=CPP_FLAGS,
        language="c++",
    ),
    make_draken_extension("vectors.date32_vector", "vectors/date32_vector.pyx"),
    make_draken_extension("vectors.timestamp_vector", "vectors/timestamp_vector.pyx"),
    make_draken_extension("morsels.morsel", "morsels/morsel.pyx"),
    # Pre-generated C module for morsels.align (Cython-generated C source)
    Extension(
        "opteryx.draken.morsels.align",
        sources=["third_party/mabel/draken/morsels/align.pyx"],
        include_dirs=include_dirs,
        extra_compile_args=C_FLAGS,
        language="c",
    ),
    # Hash API shim used by a few draken helpers (Cython wrapper)
    Extension(
        "opteryx.draken.vectors._hash_api",
        sources=[
            "opteryx/draken/vectors/_hash_api.pyx",
            "src/cpp/simd_hash.cpp",
            "src/cpp/cpu_features.cpp",
        ],
        include_dirs=include_dirs,
        language="c++",
        extra_compile_args=CPP_FLAGS,
    ),
    
    # Core compiled components
    Extension(
        "opteryx.compiled.functions.strings",
        sources=[
            "opteryx/compiled/functions/strings.pyx",
            "src/cpp/simd_search.cpp",
            "src/cpp/simd_string_ops.cpp",
            "src/cpp/cpu_features.cpp"
        ],
        include_dirs=include_dirs,
        language="c++",
        extra_compile_args=CPP_FLAGS,
    ),
    Extension(
        name="opteryx.compiled.functions.timestamp",
        sources=["opteryx/compiled/functions/timestamp.pyx"],
        extra_compile_args=C_FLAGS,
    ),
    Extension(
        name="opteryx.compiled.functions.vectors",
        sources=["opteryx/compiled/functions/vectors.pyx"],
        include_dirs=include_dirs,
        language="c++",
        extra_compile_args=CPP_FLAGS,
    ),
    Extension(
        "opteryx.compiled.simd_probe",
        sources=[
            "opteryx/compiled/simd_probe.pyx",
            "src/cpp/simd_env.cpp", 
            "src/cpp/cpu_features.cpp",
        ],
        include_dirs=include_dirs,
        language="c++",
        extra_compile_args=CPP_FLAGS,
    ),
    Extension(
        "opteryx.compiled.structures.hash_table",
        sources=["opteryx/compiled/structures/hash_table.pyx"],
        include_dirs=include_dirs,
        language="c++",
        extra_compile_args=CPP_FLAGS,
    ),
    Extension(
        "opteryx.compiled.structures.node",
        sources=["opteryx/compiled/structures/node.pyx"],
        include_dirs=include_dirs,
        extra_compile_args=C_FLAGS,
    ),
    Extension(
        "opteryx.compiled.structures.relation_statistics",
        sources=["opteryx/compiled/structures/relation_statistics.pyx"],
        include_dirs=include_dirs,
        language="c++",
        extra_compile_args=CPP_FLAGS,
    ),
    Extension(
        "opteryx.compiled.structures.bloom_filter",
        sources=["opteryx/compiled/structures/bloom_filter.pyx"],
        include_dirs=include_dirs,
        extra_compile_args=C_FLAGS,
    ),
    # MemoryViewStream: high-performance memoryview-backed stream (Cython)
    Extension(
        "opteryx.compiled.structures.memory_view_stream",
        sources=["opteryx/compiled/structures/memory_view_stream.pyx"],
        include_dirs=include_dirs,
        extra_compile_args=C_FLAGS,
        language="c",
    ),
    Extension(
        "opteryx.compiled.structures.memory_pool",
        sources=["opteryx/compiled/structures/memory_pool.pyx"],
        include_dirs=include_dirs,
        language="c++",
        extra_compile_args=CPP_FLAGS,
    ),
    Extension(
        "opteryx.compiled.structures.lru_k",
        sources=["opteryx/compiled/structures/lru_k.pyx"],
        include_dirs=include_dirs,
        language="c++",
        extra_compile_args=CPP_FLAGS,
    ),
    # C-backed integer buffer used across joins and other kernels
    Extension(
        "opteryx.compiled.structures.buffers",
        sources=[
            "opteryx/compiled/structures/buffers.pyx",
            "src/cpp/intbuffer.cpp",
            # join kernels are tightly coupled with the buffer implementation
            # build them into the same module so symbols are available at runtime
            "src/cpp/join_kernels.cpp",
        ],
        include_dirs=include_dirs,
        language="c++",
        extra_compile_args=CPP_FLAGS,
    ),
    # Aggregations: count_distinct and group-by helpers (C++ implementations)
    Extension(
        "opteryx.compiled.aggregations.count_distinct",
        sources=[
            "opteryx/compiled/aggregations/count_distinct.pyx",
        ],
        include_dirs=include_dirs,
        language="c++",
        extra_compile_args=CPP_FLAGS,
    ),
    # (group_by_draken left as a pure-Python helper; skip compiling .pyx)
    Extension(
        "opteryx.compiled.table_ops.distinct",
        sources=["opteryx/compiled/table_ops/distinct.pyx", "src/cpp/intbuffer.cpp"],
        include_dirs=include_dirs,
        language="c++",
        extra_compile_args=CPP_FLAGS,
    ),
    Extension(
        name="opteryx.compiled.table_ops.hash_ops",
        sources=["opteryx/compiled/table_ops/hash_ops.pyx"],
        include_dirs=include_dirs + ["third_party/abseil"],
        language="c++",
        extra_compile_args=CPP_FLAGS,
    ),
    Extension(
        name="opteryx.compiled.table_ops.null_avoidant_ops",
        sources=["opteryx/compiled/table_ops/null_avoidant_ops.pyx"],
        include_dirs=include_dirs,
        language="c++",
        extra_compile_args=CPP_FLAGS,
    ),
    Extension(
        "opteryx.compiled.io.disk_reader",
        sources=[
            "opteryx/compiled/io/disk_reader.pyx",
            "src/cpp/disk_io.cpp",
            "src/cpp/directories.cpp",
        ],
        include_dirs=include_dirs,
        language="c++",
        extra_compile_args=CPP_FLAGS,
    ),
]

# Auto-generate consolidated modules
def generate_consolidated_module(module_dir, output_file):
    pyx_files = sorted([
        os.path.basename(f) for f in glob.glob(os.path.join(module_dir, "*.pyx"))
        if os.path.basename(f) != os.path.basename(output_file)
    ])
    
    with open(output_file, 'w', encoding="UTF8") as f:
        f.write("# Auto-generated consolidated module\n# DO NOT EDIT - generated by setup.py\n\n")
        for pyx_file in pyx_files:
            f.write(f'include "{pyx_file}"\n')
    
    print(f"Generated {output_file} with {len(pyx_files)} includes")

# Generate list_ops and joins
generate_consolidated_module("opteryx/compiled/list_ops", "opteryx/compiled/list_ops/list_ops.pyx")
generate_consolidated_module("opteryx/compiled/joins", "opteryx/compiled/joins/joins.pyx")

# Add consolidated modules with their dependencies
# Link args for list_ops (use -lcrypto on non-macOS and -pthread where appropriate)
list_ops_link_args = []
if not is_mac():
    list_ops_link_args.append("-lcrypto")
if not is_win():
    list_ops_link_args.append("-pthread")

extensions.extend([
    Extension(
        "opteryx.compiled.list_ops.function_definitions",
        sources=(
            ["opteryx/compiled/list_ops/list_ops.pyx"]
            + sorted(glob.glob("third_party/re2/re2/*.cc") + [
                "third_party/re2/util/strutil.cc",
                "third_party/re2/util/rune.cc",
                "src/cpp/simd_env.cpp",
                "src/cpp/simd_search.cpp",
                "src/cpp/cpu_features.cpp",
            ])
        ),
        include_dirs=include_dirs,
        language="c++",
        extra_compile_args=CPP_FLAGS,
        extra_link_args=list_ops_link_args,
    ),
    Extension(
        "opteryx.compiled.joins.join_definitions", 
        sources=[
            "opteryx/compiled/joins/joins.pyx",
            "src/cpp/join_kernels.cpp",
            "src/cpp/intbuffer.cpp",
        ],
        include_dirs=include_dirs,
        language="c++",
        extra_compile_args=CPP_FLAGS,
    ),
])

# Setup configuration
setup(
    name=LIBRARY,
    version=__version__,
    description="Python SQL Query Engine",
    long_description=long_description,
    long_description_content_type="text/markdown",
    packages=find_packages(include=[LIBRARY, f"{LIBRARY}.*"]),
    python_requires=">=3.11",
    url="https://github.com/mabel-dev/opteryx/",
    ext_modules=cythonize(extensions, compiler_directives={
        "language_level": "3", 
        "linetrace": "a" in __version__ or "b" in __version__,
    }),
    rust_extensions=[RustExtension("opteryx.compute", "Cargo.toml", debug=False)],  # Add Rust here
    entry_points={"console_scripts": ["opteryx=opteryx.command:main"]},
    package_data={"": ["*.pyx", "*.pxd", "*.h"]},
    cmdclass={"build_ext": build_ext},
    zip_safe=False,
)
