#!/bin/bash

set -e
set -o pipefail

if ! test -f "customCA.crt"; then
  echo Generating Key
  openssl genrsa -des3 -passout file:passphrase.txt -out customCA.key 4096
  echo Generating CA-Cert
  openssl req -x509 -new -nodes \
    -key customCA.key \
    -sha256 -days 2048 \
    -subj "/CN=Acme" \
    -passin file:passphrase.txt \
    -out customCA.crt
fi;
