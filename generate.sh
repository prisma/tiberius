#!/usr/bin/env bash

set -e
set -o pipefail

openssl req -x509 -newkey rsa:4096 -keyout mssql.key -out mssql.crt -sha256 -nodes -days 3650 -subj /CN=tiberius -addext subjectAltName=DNS:localhost