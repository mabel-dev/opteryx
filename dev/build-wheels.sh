#!/bin/bash
set -ex

# Install OpenSSL development headers inside the container
# Note: zstd/snappy are vendored into the project; we should not install
# zstd-devel/snappy-devel via yum inside the manylinux container (they may
# not be available on the base image and we compile vendor sources directly).
yum install -y openssl-devel

# Install Rust (required for building some Python packages with Rust extensions)
curl https://sh.rustup.rs -sSf | sh -s -- --default-toolchain stable -y
export PATH="$HOME/.cargo/bin:$PATH"

cd $GITHUB_WORKSPACE/io
cd io

# Only build for the specified Python version
PYBIN="/opt/python/cp${PYTHON_VERSION//.}-cp${PYTHON_VERSION//.}/bin"

# Install necessary packages
"${PYBIN}/python" -m pip install -U setuptools wheel setuptools-rust numpy cython auditwheel draken

# Build the wheel
"${PYBIN}/python" setup.py bdist_wheel

# Repair the wheel using auditwheel
for whl in dist/*.whl; do
    auditwheel repair "$whl" -w dist/
done