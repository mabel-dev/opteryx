from typing import Any
from typing import Dict

import numpy
from Cython.Build import cythonize
from setuptools import Extension
from setuptools import find_packages
from setuptools import setup
from setuptools_rust import RustExtension

LIBRARY = "opteryx"


def rust_build(setup_kwargs: Dict[str, Any]) -> None:
    setup_kwargs.update(
        {
            "rust_extensions": [
                RustExtension("opteryx.third_party.sqloxide.sqloxide", "Cargo.toml", debug=False)
            ],
            "zip_safe": False,
        }
    )


__version__ = "notset"
with open(f"{LIBRARY}/__version__.py", mode="r") as v:
    vers = v.read()
exec(vers)  # nosec

__build__ = "notset"
with open(f"{LIBRARY}/__build__.py", mode="r") as v:
    build = v.read()
exec(vers)  # nosec

__author__ = "notset"
with open(f"{LIBRARY}/__author__.py", mode="r") as v:
    author = v.read()
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
        name="cjoin",
        sources=["opteryx/third_party/pyarrow_ops/cjoin.pyx"],
        include_dirs=[numpy.get_include()],
    ),
    Extension(
        name="csoundex",
        sources=["opteryx/third_party/fuzzy/csoundex.pyx"],
        extra_compile_args=["-O2"],
    ),
    Extension(
        name="clevenshtein",
        sources=["opteryx/third_party/levenshtein/clevenshtein.pyx"],
        extra_compile_args=["-O2"],
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
