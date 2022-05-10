import numpy as np
from Cython.Build import cythonize
from setuptools import Extension, find_packages, setup

with open("opteryx/version.py", "r") as v:
    vers = v.read()
exec(vers)  # nosec

with open("README.md", "r") as rm:
    long_description = rm.read()

try:
    with open("requirements.txt") as f:
        required = f.read().splitlines()
except FileNotFoundError:
    # this sometimes fails - so put them here, but this needs to be maintained manually
    required = ["cython", "numpy", "orjson", "cityhash", "sqloxide", "pyarrow"]

extensions = [
    Extension(
        name="cjoin",
        sources=["opteryx/third_party/pyarrow_ops/cjoin.pyx"],
        include_dirs=[np.get_include()],
    ),
    #    Extension(
    #        name="cythonize",
    #        sources=["opteryx/third_party/accumulation_tree/accumulation_tree.pyx"],
    #    )
    #    "mabel/data/internals/group_by.py",
]

setup(
    name="opteryx",
    version=__version__,
    description="Serverless SQL Engine",
    long_description=long_description,
    long_description_content_type="text/markdown",
    maintainer="Joocer",
    author="joocer",
    author_email="justin.joyce@joocer.com",
    packages=find_packages(include=["opteryx", "opteryx.*"]),
    url="https://github.com/mabel-dev/opteryx/",
    install_requires=required,
    ext_modules=cythonize(extensions),
)
