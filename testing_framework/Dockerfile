# This Dockerfile pulls a python 3.8 image and installs necessary packages with poetry
# (see https://stackoverflow.com/a/54830677)
FROM python:3.8

# For pyspark configurations
#FROM openjdk:8-slim
#USER root

# === Set environment variables.
# Directory where build commands are run
ENV TEST_DIRECTORY=/tests/
# Suppress the ouput of python byte code
ENV PYTHONDONTWRITEBYTECODE='1'
# Suppress pip's reminder to update its version
ENV PIP_DISABLE_PIP_VERSION_CHECK=on
# Set whether to install dependencies globally.
# `false` allows tests to be run through Jenkins.
ENV POETRY_VIRTUALENVS_CREATE=false
# Supress `pip` caching
ENV PIP_NO_CACHE_DIR=true

# Set up python environment variables and paths                                                 \
ENV PYTHONPATH=/usr/bin/python3
ENV PYENV_ROOT=/.pyenv
ENV PATH=$PYENV_ROOT/bin:$PATH
ENV PYENV_SHELL=/bin/bash
ENV PATH=/.pyenv/shims:${PATH}

# Install pyenv dependencies
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        git make build-essential libssl-dev zlib1g-dev \
        libbz2-dev libreadline-dev libsqlite3-dev wget \
        curl llvm libncurses5-dev xz-utils tk-dev libxml2-dev \
        libxmlsec1-dev libffi-dev liblzma-dev

# Install pyenv
RUN git clone https://github.com/pyenv/pyenv.git /.pyenv
RUN pyenv init -
RUN pyenv install 3.8.0
RUN pyenv global 3.8.0

# Install the Python dependencies.
RUN python3 -m pip install --upgrade pip

# Create softlink from bash shell since Docker uses sh
RUN ln -sf /bin/bash /bin/sh

# Set the working directory
WORKDIR ${TEST_DIRECTORY}

# Install poetry
RUN pip install poetry

# Copy in the files needed to set up the work environment
COPY poetry.lock pyproject.toml ${TEST_DIRECTORY}

RUN poetry install --no-interaction
