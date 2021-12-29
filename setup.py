from setuptools import setup
from setuptools import find_packages
from Cython.Build import cythonize


with open("opteryx/version.py", "r") as v:
    vers = v.read()
exec(vers)  # nosec

with open("README.md", "r") as rm:
    long_description = rm.read()

with open("requirements.txt") as f:
    required = f.read().splitlines()


setup(
    name="opteryx",
    version=__version__,
    description="Distributed SQL Engine",
    long_description=long_description,
    long_description_content_type="text/markdown",
    maintainer="Joocer",
    author="joocer",
    author_email="justin.joyce@joocer.com",
    packages=find_packages(include=["opteryx", "opteryx.*"]),
    url="https://github.com/mabel-dev/opteryx/",
    install_requires=required,
    ext_modules=cythonize(
        [
            #    "mabel/data/internals/group_by.py",
            #    "mabel/data/internals/dictset.py",
            #    "mabel/data/internals/expression.py",
            #    "mabel/data/readers/internals/inline_evaluator.py",
            #    "mabel/data/readers/internals/parallel_reader.py",
            #    "mabel/data/internals/relation.py",
            #    "mabel/data/internals/bloom_filter.py",
            #    "mabel/utils/uintset/uintset.py"
            "opteryx/imports/accumulation_tree/accumulation_tree.pyx"
        ]
    ),
)
