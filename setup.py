import platform
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


if is_mac():
    COMPILE_FLAGS = ["-O2"]
else:
    COMPILE_FLAGS = ["-O2", "-march=native"]


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
        name="csoundex",
        sources=["opteryx/third_party/fuzzy/csoundex.pyx"],
        extra_compile_args=COMPILE_FLAGS,
    ),
    Extension(
        name="clevenshtein",
        sources=["opteryx/compiled/levenshtein/clevenshtein.pyx"],
        extra_compile_args=COMPILE_FLAGS,
    ),
    Extension(
        name="cython_list_ops",
        sources=[
            "opteryx/compiled/list_ops/cython_list_ops.pyx",
        ],
        include_dirs=[numpy.get_include()],
        extra_compile_args=COMPILE_FLAGS,
    ),
    Extension(
        name="cython_cross_join",
        sources=["opteryx/compiled/cross_join/cython_cross_join.pyx"],
        include_dirs=[numpy.get_include()],
        extra_compile_args=COMPILE_FLAGS,
    ),
    Extension(
        name="cython_functions",
        sources=["opteryx/compiled/functions/cython_functions.pyx"],
        include_dirs=[numpy.get_include()],
        extra_compile_args=COMPILE_FLAGS,
    ),
    Extension(
        name="ip_address",
        sources=["opteryx/compiled/functions/ip_address.pyx"],
        include_dirs=[numpy.get_include()],
        extra_compile_args=COMPILE_FLAGS,
    ),
    Extension(
        name="hash_table",
        sources=["opteryx/compiled/functions/hash_table.pyx"],
        include_dirs=[numpy.get_include()],
        language="c++",
        extra_compile_args=COMPILE_FLAGS + ["-std=c++11"],
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
}

rust_build(setup_config)

setup(**setup_config)
