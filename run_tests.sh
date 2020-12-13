#! /usr/bin/env bash

docker-compose build
docker-compose run black .
docker-compose run isort .

docker-compose run --rm unit-tests

docker-compose down --remove-orphans
