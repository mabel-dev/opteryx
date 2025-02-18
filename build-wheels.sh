#!/bin/bash
set -ex

curl https://sh.rustup.rs -sSf | sh -s -- --default-toolchain stable -y
export PATH="$HOME/.cargo/bin:$PATH"

# Install Apache Arrow
apt-get update
apt-get install -y libarrow-dev

cd $GITHUB_WORKSPACE/io
cd io

# Only build for the specified Python version
PYBIN="/opt/python/cp${PYTHON_VERSION//.}-cp${PYTHON_VERSION//.}/bin"

# Install necessary packages
"${PYBIN}/python" -m pip install -U setuptools wheel setuptools-rust numpy cython pyarrow
"${PYBIN}/python" -c "import pyarrow; pyarrow.create_library_symlinks();"

# Build the wheel
"${PYBIN}/python" setup.py bdist_wheel

# Repair the wheel using auditwheel
for whl in dist/*.whl; do
    auditwheel repair "$whl" -w dist/
done