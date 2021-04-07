#!/usr/bin/env bash

set -e
python -m venv .venv
source "$PWD/.venv/bin/activate"
pip install --upgrade pip
pip install -r src/requirements.txt
