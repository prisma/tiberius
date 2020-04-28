#!/bin/bash

MSSQL_SA_PASSWORD="<YourStrong@Passw0rd>"

docker network create test-net

docker run --name test-mssql \
    -e ACCEPT_EULA=Y \
    -e SA_PASSWORD=$MSSQL_SA_PASSWORD \
    -p 1433:1433 \
    -d mcr.microsoft.com/mssql/server:2019-CU3-ubuntu-18.04

docker run -w /build --network test-net -v $BUILDKITE_BUILD_CHECKOUT_PATH:/build \
    -e TIBERIUS_TEST_HOST=test-mssql \
    -e TIBERIUS_TEST_USER=SA \
    -e TIBERIUS_TEST_PW=$MSSQL_SA_PASSWORD \
    prismagraphql/build:test cargo test --all-features

exit_code=$?

docker stop test-mssql
docker rm test-mssql

docker network rm test-net

exit $exit_code
