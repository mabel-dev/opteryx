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
import shutil
import subprocess
import sys
from sysconfig import get_config_var
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


def is_win():  # pragma: no cover
    return platform.system().lower() == "windows"


def check_rust_availability():  # pragma: no cover
    """
    Check if Rust toolchain is available and return paths.

    Returns:
        tuple: (rustc_path, cargo_path, rust_env) if available

    Raises:
        SystemExit: If Rust is not available with a helpful error message
    """
    # Check environment variables first, treating empty strings as None
    rustc_path = os.environ.get("RUSTC") or None
    cargo_path = os.environ.get("CARGO") or None

    # If not set via environment, check PATH
    if not rustc_path:
        rustc_path = shutil.which("rustc")
    if not cargo_path:
        cargo_path = shutil.which("cargo")

    # Validate that paths exist if provided
    if rustc_path and not os.path.isfile(rustc_path):
        print(
            f"\033[38;2;255;208;0mWarning:\033[0m RUSTC environment variable points to non-existent file: {rustc_path}",
            file=sys.stderr,
        )
        rustc_path = None
    if cargo_path and not os.path.isfile(cargo_path):
        print(
            f"\033[38;2;255;208;0mWarning:\033[0m CARGO environment variable points to non-existent file: {cargo_path}",
            file=sys.stderr,
        )
        cargo_path = None

    if not rustc_path or not cargo_path:
        error_msg = """
\033[38;2;255;85;85m╔═══════════════════════════════════════════════════════════════════════════╗
║                         RUST TOOLCHAIN NOT FOUND                          ║
╚═══════════════════════════════════════════════════════════════════════════╝\033[0m

Opteryx requires the Rust compiler (rustc) and Cargo to build native extensions.

\033[38;2;255;208;0mWhat's missing:\033[0m
"""
        if not rustc_path:
            error_msg += "  • rustc (Rust compiler) not found in PATH\n"
        if not cargo_path:
            error_msg += "  • cargo (Rust package manager) not found in PATH\n"

        error_msg += """
\033[38;2;255;208;0mHow to install Rust:\033[0m

  1. Visit: https://rustup.rs/
  2. Run the installation command for your platform:
     
     \033[38;2;139;233;253mLinux/macOS:\033[0m
       curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
     
     \033[38;2;139;233;253mWindows:\033[0m
       Download and run: https://win.rustup.rs/
  
  3. After installation, restart your terminal or run:
       source $HOME/.cargo/env
  
  4. Verify installation:
       rustc --version
       cargo --version

\033[38;2;255;208;0mAlternative:\033[0m
  If Rust is installed but not in PATH, you can set the CARGO and RUSTC 
  environment variables before running setup.py:
  
    export CARGO=/path/to/cargo
    export RUSTC=/path/to/rustc

\033[38;2;255;85;85m═══════════════════════════════════════════════════════════════════════════\033[0m
"""
        print(error_msg, file=sys.stderr)
        sys.exit(1)

    # Get Rust version for informational purposes
    try:
        rust_version = subprocess.check_output(
            [rustc_path, "--version"], text=True, stderr=subprocess.STDOUT
        ).strip()
        print(f"\033[38;2;139;233;253mFound Rust toolchain:\033[0m {rust_version}")
        print(f"\033[38;2;139;233;253mrustc path:\033[0m {rustc_path}")
        print(f"\033[38;2;139;233;253mcargo path:\033[0m {cargo_path}")
    except (subprocess.CalledProcessError, OSError) as e:
        # If we can't run rustc --version, the path might not be a valid rustc binary
        print(
            f"\033[38;2;255;208;0mWarning:\033[0m Could not verify Rust installation at {rustc_path}: {e}",
            file=sys.stderr,
        )

    # Create environment dict with explicit paths
    rust_env = {
        "RUSTC": rustc_path,
        "CARGO": cargo_path,
    }

    return rustc_path, cargo_path, rust_env


REQUESTED_COMMANDS = {arg.lower() for arg in sys.argv[1:] if arg and not arg.startswith("-")}
SHOULD_BUILD_EXTENSIONS = "clean" not in REQUESTED_COMMANDS

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

    # Check Rust availability and get paths
    rustc_path, cargo_path, rust_env = check_rust_availability()

    def rust_build(setup_kwargs: Dict[str, Any]) -> None:
        setup_kwargs.update(
            {
                "rust_extensions": [
                    RustExtension("opteryx.compute", "Cargo.toml", debug=False, env=rust_env)
                ],
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
            sources=["opteryx/third_party/cyan4973/xxhash.pyx", "third_party/cyan4973/xxhash.c"],
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
            name="opteryx.compiled.functions.functions",
            sources=["opteryx/compiled/functions/functions.pyx"],
            include_dirs=include_dirs,
            extra_compile_args=C_COMPILE_FLAGS,
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
            name="opteryx.compiled.structures.jsonl_decoder",
            sources=["opteryx/compiled/structures/jsonl_decoder.pyx", "src/cpp/simd_search.cpp"],
            include_dirs=include_dirs + ["third_party/fastfloat/fast_float"],
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

    # Auto-generate functions.pyx to include all individual .pyx files in the folder
    # This ensures new files are automatically included when added
    functions_dir = "opteryx/compiled/functions"
    functions_file = os.path.join(functions_dir, "functions.pyx")

    # Find all .pyx files in the functions directory (excluding functions.pyx itself)
    functions_pyx_files = sorted(
        [
            os.path.basename(f)
            for f in glob.glob(os.path.join(functions_dir, "*.pyx"))
            if os.path.basename(f) != "functions.pyx"
        ]
    )

    # Generate the functions.pyx file with include directives
    with open(functions_file, "w", encoding="UTF8") as f:
        f.write("""# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False

\"\"\"
Auto-generated consolidated functions module.
This file is automatically generated by setup.py and includes all individual
function files in the functions directory.

DO NOT EDIT THIS FILE MANUALLY - it will be overwritten during build.
\"\"\"

""")

        # Add include directives for each .pyx file
        for pyx_file in functions_pyx_files:
            f.write(f'include "{pyx_file}"\n')

    print(
        f"\033[38;2;189;147;249mAuto-generated functions.pyx with {len(functions_pyx_files)} includes\033[0m"
    )

    list_ops_link_args = []
    if not is_mac():
        list_ops_link_args.append("-lcrypto")

    extensions.append(
        Extension(
            name="opteryx.compiled.list_ops.function_definitions",
            sources=[list_ops_file, "src/cpp/simd_search.cpp"],
            language="c++",
            include_dirs=include_dirs
            + ["third_party/abseil", "third_party/apache", "opteryx/third_party/apache"],
            extra_compile_args=CPP_COMPILE_FLAGS,
            extra_link_args=list_ops_link_args,
        ),
    )

    extensions.append(
        Extension(
            name="opteryx.compiled.joins.join_definitions",
            sources=[joins_file],
            language="c++",
            include_dirs=include_dirs + ["third_party/abseil", "third_party/fastfloat/fast_float"],
            extra_compile_args=CPP_COMPILE_FLAGS,
        ),
    )

    extensions.append(
        Extension(
            name="opteryx.compiled.functions.function_definitions",
            sources=[functions_file],
            language="c++",
            include_dirs=include_dirs,
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
            "": ["*.pyx", "*.pxd"],
        },
    }

    rust_build(setup_config)

    setup(**setup_config)
