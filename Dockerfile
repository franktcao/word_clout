# This Dockerfile pulls an image of python 3.6 for the virtual environment
#   and installs the necessary packages using poetry.
# The template of this Dockerfile was inspired by this StackOverflow post: https://stackoverflow.com/a/54830677
#FROM openjdk:8-slim
FROM python:3.8

## Docker needs root privileges to install the necessary packages.
#USER root

# Set environment variables.
#   TEST_DIRECTORY                  - Explicity set the directory where these build
#                                     commands are being run.
#   PYTHONDONTWRITEBYTECODE         - Suppress the ouput of python byte code.
#   PIP_DISABLE_PIP_VERSION_CHECK   - Suppress pip's reminder to update its version.
#   POETRY_VIRTUALENVS_CREATE       - Set whether to install dependencies globally.
#                                     Setting this to `false` or the equivalent allows
#                                     the tests to be run through Jenkins.
#   PIP_CACHE_DIR               - Supress `pip` caching.
ENV \
  TEST_DIRECTORY=/src/              \
  PYTHONDONTWRITEBYTECODE='1'       \
  PIP_DISABLE_PIP_VERSION_CHECK=on  \
  POETRY_VIRTUALENVS_CREATE=false   \
  PIP_NO_CACHE_DIR=true

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


RUN git clone https://github.com/pyenv/pyenv.git /.pyenv
RUN pyenv init -
RUN pyenv install 3.8.0
RUN pyenv global 3.8.0

# Install the Python dependencies.
RUN python3 -m pip install --upgrade pip

# Links the bash shell to the sh shell because Docker uses sh.
RUN ln -sf /bin/bash /bin/sh

# Use pip to install poetry. Doing so in this manner is fine since this image is
# basically a virtual environment.
RUN pip install poetry

# Explicitly set the working directory and copy in the files needed to set up the work
# environment.
WORKDIR ${TEST_DIRECTORY}
COPY poetry.lock pyproject.toml ${TEST_DIRECTORY}
# Note: Any files added here are read only to Jenkins. You should not copy in files that
# are needed to run your tests.

RUN poetry install --no-interaction
CMD ["echo", "'hhi"]
