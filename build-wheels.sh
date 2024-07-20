#!/bin/bash
set -ex

curl https://sh.rustup.rs -sSf | sh -s -- --default-toolchain stable -y
export PATH="$HOME/.cargo/bin:$PATH"

cd $GITHUB_WORKSPACE/io
cd io

PYTHON_VERSIONS=("cp39" "cp310" "cp311" "cp312")

for PYBIN in /opt/python/${PYTHON_VERSIONS[@]/#/cp}/bin; do
    if [[ "${PYBIN}" == *"${PYTHON_VERSION}"* ]]; then
        "${PYBIN}/pip" install -U setuptools wheel setuptools-rust numpy==1.* cython
        "${PYBIN}/python" setup.py bdist_wheel
    fi
done

for whl in dist/*.whl; do
    auditwheel repair "$whl" -w dist/
done