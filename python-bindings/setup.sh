#!/bin/bash
SYS_PYTHON="$PYTHON"
if [[ -z $SYS_PYTHON  ]]; then
    SYS_PYTHON="python"
fi
ENV_PYTHON="./env-maturin/bin/python"
if [[ ! -e env-maturin ]]; then
    echo "Creating environment and installing maturin"
    $SYS_PYTHON -m venv env-maturin
    $ENV_PYTHON -m pip install --upgrade pip maturin
    echo "*" >> env-maturin/.gitignore
else
    echo "Environment exists, not creating"
fi

if [[ ! -e Cargo.toml ]]; then
    echo "Initializing maturin project"
    ./env-maturin/bin/maturin init --name "py_ginput_bindings" --bindings "pyo3" .
    rm -r .github
else
    echo "Cargo.toml detected, not initializing project"
fi