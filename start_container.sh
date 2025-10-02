#!/bin/bash

set -euo pipefail

ENGINE=$1 
NAME="mssql-$ENGINE"
PORT=1433

case "$ENGINE" in
  2017) IMAGE="mcr.microsoft.com/mssql/server:2017-latest" ;;
  2019) IMAGE="mcr.microsoft.com/mssql/server:2019-latest" ;;
  2022) IMAGE="mcr.microsoft.com/mssql/server:2022-latest" ;;
  azure) IMAGE="mcr.microsoft.com/azure-sql-edge:latest" ;;
  *)
    echo "Usage: $0 {2017|2019|2022|azure}"
    exit 1
    ;;
esac

echo "Starting $NAME using $IMAGE"

docker kill "$NAME" 2>/dev/null || true
docker rm "$NAME" 2>/dev/null || true

docker run -d \
  --name "$NAME" \
  -e "ACCEPT_EULA=Y" \
  -e "SA_PASSWORD=<YourStrong@Passw0rd>" \
  -p $PORT:1433 \
  "$IMAGE"
