#!/bin/bash

MSSQL_SA_PASSWORD="<YourStrong@Passw0rd>"

docker network create test-net

docker run --name test-mssql --network test-net \
    -e ACCEPT_EULA=Y \
    -e SA_PASSWORD=$MSSQL_SA_PASSWORD \
    -p 1433:1433 \
    -d mcr.microsoft.com/mssql/server:2019-CU3-ubuntu-18.04

docker run -w /build --network test-net -v $BUILDKITE_BUILD_CHECKOUT_PATH:/build \
    -e RUSTFLAGS="-D warnings" \
    -e TIBERIUS_TEST_CONNECTION_STRING="server=tcp:test-mssql,1433;user=SA;password=$MSSQL_SA_PASSWORD;TrustServerCertificate=true" \
    prismagraphql/build:test cargo test $1 -- --test-threads=1

exit_code=$?

docker stop test-mssql
docker rm test-mssql

docker network rm test-net

exit $exit_code
