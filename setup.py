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
from pathlib import Path
from sysconfig import get_config_var
from typing import Any
from typing import Dict
from typing import List

import numpy
from Cython.Build import cythonize
from setuptools import Extension
from setuptools import find_packages
from setuptools import setup
from setuptools.command.build_ext import build_ext as build_ext_orig
from setuptools_rust import RustExtension

LIBRARY = "opteryx"

ROOT_DIR = Path(__file__).parent.resolve()
VENDOR_ROOT = ROOT_DIR / "third_party" / "mabel"
VENDORED_DEPENDENCIES = ("draken", "rugo")


def is_mac():  # pragma: no cover
    return platform.system().lower() == "darwin"


def is_win():  # pragma: no cover
    return platform.system().lower() == "windows"


def is_linux():  # pragma: no cover
    return platform.system().lower() == "linux"


def resolve_integrated_packages(names: List[str]) -> Dict[str, Path]:
    """Resolve in-tree copies of external packages."""

    resolved: Dict[str, Path] = {}
    for name in names:
        candidate = VENDOR_ROOT / name
        if not candidate.exists():
            raise FileNotFoundError(
                f"Integrated dependency '{name}' not found at {candidate}."
            )
        resolved[name] = candidate

    return resolved


class VendorAwareBuildExt(build_ext_orig):
    """Custom build_ext that recognises vendored assembly sources."""

    def build_extensions(self):
        compiler = getattr(self, "compiler", None)
        if compiler is not None:
            src_exts = getattr(compiler, "src_extensions", None)
            if src_exts is not None and ".S" not in src_exts:
                src_exts.append(".S")
        super().build_extensions()


def make_draken_extensions(draken_root: Path) -> List[Extension]:
    """Create Extension definitions for the vendored Draken package."""

    local_machine = platform.machine().lower()
    local_system = platform.system().lower()
    cpp_flags = ["-O3"]
    c_flags = ["-O3"]

    if is_mac():
        cpp_flags += ["-std=c++17"]
    elif is_win():
        cpp_flags += ["/std:c++17"]
    else:
        cpp_flags += ["-std=c++17", "-fvisibility=default"]
        c_flags += ["-fvisibility=default"]
        if "x86" in local_machine or "amd64" in local_machine:
            cpp_flags.append("-mavx2")
            c_flags.append("-mavx2")

    if (
        local_machine.startswith("arm")
        and not local_machine.startswith("aarch64")
        and local_system != "darwin"
    ):
        cpp_flags.append("-mfpu=neon")

    draken_include_dirs: List[str] = []
    include_dir = get_config_var("INCLUDEDIR")
    if include_dir:
        candidate = Path(include_dir) / "c++" / "v1"
        if candidate.exists():
            draken_include_dirs.append(str(candidate))

    include_py = get_config_var("INCLUDEPY")
    if include_py and os.path.exists(include_py):
        draken_include_dirs.append(include_py)

    draken_include_dirs.append(str(draken_root))

    def dpath(*parts: str) -> str:
        return str(draken_root.joinpath(*parts))

    draken_extensions: List[Extension] = [
        Extension(
            "draken.interop.arrow",
            sources=[dpath("interop", "arrow.pyx")],
            extra_compile_args=c_flags,
            include_dirs=draken_include_dirs,
            depends=[
                dpath("core", "buffers.h"),
                dpath("interop", "arrow_c_data_interface.h"),
            ],
        ),
        Extension(
            name="draken.vectors.vector",
            sources=[dpath("vectors", "vector.pyx")],
            extra_compile_args=c_flags,
            include_dirs=draken_include_dirs,
            depends=[dpath("core", "buffers.h")],
        ),
        Extension(
            name="draken.core.ops",
            sources=[dpath("core", "ops.pyx"), dpath("core", "ops_impl.cpp")],
            extra_compile_args=cpp_flags,
            include_dirs=draken_include_dirs,
            depends=[
                dpath("core", "buffers.h"),
                dpath("core", "ops.h"),
            ],
            language="c++",
        ),
        Extension(
            name="draken.vectors.bool_vector",
            sources=[dpath("vectors", "bool_vector.pyx")],
            extra_compile_args=c_flags,
            include_dirs=draken_include_dirs,
            depends=[dpath("core", "buffers.h")],
        ),
        Extension(
            name="draken.vectors.float64_vector",
            sources=[dpath("vectors", "float64_vector.pyx")],
            extra_compile_args=c_flags,
            include_dirs=draken_include_dirs,
            depends=[dpath("core", "buffers.h")],
        ),
        Extension(
            name="draken.vectors.int64_vector",
            sources=[dpath("vectors", "int64_vector.pyx")],
            extra_compile_args=c_flags,
            include_dirs=draken_include_dirs,
            depends=[dpath("core", "buffers.h")],
        ),
        Extension(
            name="draken.vectors.string_vector",
            sources=[dpath("vectors", "string_vector.pyx")],
            extra_compile_args=c_flags,
            include_dirs=draken_include_dirs,
            depends=[dpath("core", "buffers.h")],
        ),
        Extension(
            name="draken.vectors.date32_vector",
            sources=[dpath("vectors", "date32_vector.pyx")],
            extra_compile_args=c_flags,
            include_dirs=draken_include_dirs,
            depends=[dpath("core", "buffers.h")],
        ),
        Extension(
            name="draken.vectors.timestamp_vector",
            sources=[dpath("vectors", "timestamp_vector.pyx")],
            extra_compile_args=c_flags,
            include_dirs=draken_include_dirs,
            depends=[dpath("core", "buffers.h")],
        ),
        Extension(
            name="draken.vectors.time_vector",
            sources=[dpath("vectors", "time_vector.pyx")],
            extra_compile_args=c_flags,
            include_dirs=draken_include_dirs,
            depends=[dpath("core", "buffers.h")],
        ),
        Extension(
            name="draken.vectors.array_vector",
            sources=[dpath("vectors", "array_vector.pyx")],
            extra_compile_args=c_flags,
            include_dirs=draken_include_dirs,
            depends=[dpath("core", "buffers.h")],
        ),
        Extension(
            name="draken.morsels.morsel",
            sources=[dpath("morsels", "morsel.pyx")],
            extra_compile_args=c_flags,
            include_dirs=draken_include_dirs,
            depends=[
                dpath("core", "buffers.h"),
                dpath("morsels", "morsel.h"),
            ],
        ),
    ]

    return draken_extensions


def make_rugo_extensions(rugo_root: Path) -> List[Extension]:
    """Create Extension definitions for the vendored Rugo package."""

    extra_compile_args = ["-O3", "-std=c++17"]
    if platform.system() == "Darwin":
        default_arch = platform.machine()
        archs = os.environ.get("CIBW_ARCHS_MACOS", default_arch).split()
        for arch in archs:
            arch = arch.strip()
            if not arch or arch.lower() in {"auto", "native", "none"}:
                continue
            extra_compile_args.extend(["-arch", arch])

    def detect_target_machine() -> str:
        for key in ("CIBW_ARCHS", "CIBW_ARCH", "CIBW_BUILD"):
            val = os.environ.get(key)
            if not val:
                continue
            lowered = val.lower()
            if "aarch64" in lowered or "arm64" in lowered:
                return "aarch64"
            if "x86_64" in lowered or "amd64" in lowered or "x86" in lowered:
                return "x86_64"
        return platform.machine().lower()

    target_machine = detect_target_machine()

    vendor_sources: List[str] = [
        str(rugo_root / "parquet" / "vendor" / "snappy" / "snappy.cc"),
        str(rugo_root / "parquet" / "vendor" / "snappy" / "snappy-sinksource.cc"),
        str(rugo_root / "parquet" / "vendor" / "snappy" / "snappy-stubs-internal.cc"),
        str(rugo_root / "parquet" / "vendor" / "zstd" / "common" / "entropy_common.cpp"),
        str(rugo_root / "parquet" / "vendor" / "zstd" / "common" / "fse_decompress.cpp"),
        str(rugo_root / "parquet" / "vendor" / "zstd" / "common" / "zstd_common.cpp"),
        str(rugo_root / "parquet" / "vendor" / "zstd" / "common" / "xxhash.cpp"),
        str(rugo_root / "parquet" / "vendor" / "zstd" / "common" / "error_private.cpp"),
        str(rugo_root / "parquet" / "vendor" / "zstd" / "decompress" / "zstd_decompress.cpp"),
        str(rugo_root / "parquet" / "vendor" / "zstd" / "decompress" / "zstd_decompress_block.cpp"),
        str(rugo_root / "parquet" / "vendor" / "zstd" / "decompress" / "huf_decompress.cpp"),
        str(rugo_root / "parquet" / "vendor" / "zstd" / "decompress" / "zstd_ddict.cpp"),
    ]

    if target_machine in {"x86_64", "amd64"}:
        vendor_sources.append(
            str(rugo_root / "parquet" / "vendor" / "zstd" / "decompress" / "huf_decompress_amd64.S")
        )

    parquet_sources = [
        str(rugo_root / "parquet" / "parquet_reader.pyx"),
        str(rugo_root / "parquet" / "metadata.cpp"),
        str(rugo_root / "parquet" / "bloom_filter.cpp"),
        str(rugo_root / "parquet" / "decode.cpp"),
        str(rugo_root / "parquet" / "compression.cpp"),
    ] + vendor_sources

    parquet_include_dirs = [
        str(rugo_root / "parquet" / "vendor" / "snappy"),
        str(rugo_root / "parquet" / "vendor" / "zstd"),
        str(rugo_root / "parquet" / "vendor" / "zstd" / "common"),
        str(rugo_root / "parquet" / "vendor" / "zstd" / "decompress"),
    ]

    rugo_extensions: List[Extension] = [
        Extension(
            "rugo.parquet",
            sources=parquet_sources,
            include_dirs=parquet_include_dirs,
            define_macros=[
                ("HAVE_SNAPPY", "1"),
                ("HAVE_ZSTD", "1"),
                ("ZSTD_STATIC_LINKING_ONLY", "1"),
            ],
            language="c++",
            extra_compile_args=extra_compile_args,
            extra_link_args=[],
        )
    ]

    jsonl_compile_args = extra_compile_args.copy()
    if target_machine in {"x86_64", "amd64"}:
        jsonl_compile_args.extend(["-msse4.2", "-mavx2"])

    jsonl_sources = [
        str(rugo_root / "jsonl" / "jsonl_reader.pyx"),
        str(rugo_root / "jsonl" / "decode.cpp"),
        str(rugo_root / "jsonl" / "simdjson_wrapper.cpp"),
    ]

    jsonl_include_dirs = [
        str(rugo_root / "jsonl"),
        str(rugo_root / "jsonl" / "vendor" / "simdjson" / "include"),
        str(rugo_root / "jsonl" / "vendor" / "simdjson"),
    ]

    rugo_extensions.append(
        Extension(
            "rugo.jsonl",
            sources=jsonl_sources,
            include_dirs=jsonl_include_dirs,
            language="c++",
            extra_compile_args=jsonl_compile_args,
            extra_link_args=[],
        )
    )

    csv_compile_args = extra_compile_args.copy()
    if target_machine in {"x86_64", "amd64"}:
        csv_compile_args.extend(["-msse4.2", "-mavx2"])

    csv_sources = [
        str(rugo_root / "csv" / "csv_reader.pyx"),
        str(rugo_root / "csv" / "csv_parser.cpp"),
    ]

    csv_include_dirs = [str(rugo_root / "csv")]

    rugo_extensions.append(
        Extension(
            "rugo.csv",
            sources=csv_sources,
            include_dirs=csv_include_dirs,
            language="c++",
            extra_compile_args=csv_compile_args,
            extra_link_args=[],
        )
    )

    return rugo_extensions


def collect_vendor_package_data(package_root: Path, patterns: List[str]) -> List[str]:
    """Collect relative file paths for inclusion in package data."""

    collected: set[str] = set()
    for pattern in patterns:
        for candidate in package_root.rglob(pattern):
            if candidate.is_file():
                collected.add(str(candidate.relative_to(package_root)))

    return sorted(collected)

REQUESTED_COMMANDS = {arg.lower() for arg in sys.argv[1:] if arg and not arg.startswith("-")}
SHOULD_BUILD_EXTENSIONS = "clean" not in REQUESTED_COMMANDS

if not SHOULD_BUILD_EXTENSIONS:
    REQUESTED_DISPLAY = ", ".join(sorted(REQUESTED_COMMANDS)) or "<none>"
    print(
        f"\033[38;2;255;208;0mSkipping native extension build for command(s):\033[0m {REQUESTED_DISPLAY}"
    )

vendored_paths: Dict[str, Path] = {}
try:
    vendored_paths = resolve_integrated_packages(list(VENDORED_DEPENDENCIES))
except FileNotFoundError as vendor_error:
    if SHOULD_BUILD_EXTENSIONS:
        raise
    print(f"\033[38;2;255;170;0m{vendor_error}\033[0m")

if SHOULD_BUILD_EXTENSIONS:
    CPP_COMPILE_FLAGS = ["-O3"]
    C_COMPILE_FLAGS = ["-O3"]
    if is_mac():
        CPP_COMPILE_FLAGS += ["-std=c++17"]
    elif is_win():
        CPP_COMPILE_FLAGS += ["/std:c++17"]
    else:
        CPP_COMPILE_FLAGS += ["-std=c++17", "-march=native", "-fvisibility=default"]
        C_COMPILE_FLAGS += ["-march=native", "-fvisibility=default"]

    COMMON_WARNING_SUPPRESSIONS = [
        "-Wno-unused-function",
        "-Wno-unreachable-code-fallthrough",
        "-Wno-sign-compare",
        "-Wno-integer-overflow",
        "-Wno-unused-command-line-argument",
    ]
    C_COMPILE_FLAGS.extend(COMMON_WARNING_SUPPRESSIONS)
    CPP_COMPILE_FLAGS.extend(COMMON_WARNING_SUPPRESSIONS)

    # Dynamically get the default include paths
    include_dirs = [numpy.get_include(), "src/cpp", "src/c"]

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
    COMPILER_DIRECTIVES.update(
        {
            "boundscheck": False,
            "wraparound": False,
            "initializedcheck": False,
            "cdivision": True,
            "infer_types": True,
            "nonecheck": False,
        }
    )

    print(f"\033[38;2;255;85;85mBuilding Opteryx version:\033[0m {__version__}")
    print(f"\033[38;2;255;85;85mStatus:\033[0m (test)", "(rc)" if RELEASE_CANDIDATE else "")

    with open("README.md", mode="r", encoding="UTF8") as rm:
        long_description = rm.read()

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
            sources=["opteryx/compiled/io/disk_reader.pyx", "src/cpp/disk_io.cpp"],
            include_dirs=include_dirs + ["src/cpp"],
            language="c++",
            extra_compile_args=CPP_COMPILE_FLAGS,
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

    if "draken" in vendored_paths:
        extensions.extend(make_draken_extensions(vendored_paths["draken"]))

    if "rugo" in vendored_paths:
        extensions.extend(make_rugo_extensions(vendored_paths["rugo"]))

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

    base_packages = find_packages(include=[LIBRARY, f"{LIBRARY}.*"])
    vendor_packages: List[str] = []
    vendor_include_patterns: List[str] = []
    for vendored_name in vendored_paths:
        vendor_include_patterns.extend([vendored_name, f"{vendored_name}.*"])

    if vendor_include_patterns:
        vendor_packages = find_packages(where=str(VENDOR_ROOT), include=vendor_include_patterns)

    packages = sorted(set(base_packages + vendor_packages))

    package_dir = {"": "."}
    for pkg_name, pkg_path in vendored_paths.items():
        package_dir[pkg_name] = os.path.relpath(pkg_path, ROOT_DIR)

    package_data: Dict[str, List[str]] = {
        "": ["*.pyx", "*.pxd", "*.pxi", "*.h", "*.hpp", "*.c", "*.cc", "*.cpp", "*.S"],
    }

    vendor_file_patterns = [
        "*.pyx",
        "*.pxd",
        "*.pxi",
        "*.h",
        "*.hpp",
        "*.c",
        "*.cc",
        "*.cpp",
        "*.cxx",
        "*.S",
        "*.json",
        "*.md",
        "*.txt",
        "*.rst",
    ]

    for pkg_name, pkg_path in vendored_paths.items():
        package_data[pkg_name] = collect_vendor_package_data(pkg_path, vendor_file_patterns)

    # Build include paths for Cython to find vendored .pxd files
    cython_include_paths = [str(VENDOR_ROOT)]
    
    setup_config = {
        "name": LIBRARY,
        "version": __version__,
        "description": "Python SQL Query Engine",
        "long_description": long_description,
        "long_description_content_type": "text/markdown",
        "packages": packages,
        "package_dir": package_dir,
        "python_requires": ">=3.11",
        "url": "https://github.com/mabel-dev/opteryx/",
        "ext_modules": (
            cythonize(
                extensions, 
                compiler_directives=COMPILER_DIRECTIVES,
                include_path=cython_include_paths
            )
            if SHOULD_BUILD_EXTENSIONS
            else []
        ),
        "entry_points": {
            "console_scripts": ["opteryx=opteryx.command:main"],
        },
        "package_data": package_data,
        "include_package_data": True,
        "cmdclass": {"build_ext": VendorAwareBuildExt},
    }

    rust_build(setup_config)

    setup(**setup_config)
