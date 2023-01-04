#!/usr/bin/env bash

set -e
set -o pipefail

# Skript creates a custom-signed certificate
# Parameter1 = name of the cert

CERT_KEY_NAME=$1
CERT_FILE=$CERT_KEY_NAME.crt

export CERT_CN=$CERT_KEY_NAME

echo Prepare Signing-Request-Config from Template
cat signing-request.config.template | envsubst >> src.txt

echo Generate Private-Key and Certificate-Signing-Request for $CERT_KEY_NAME
openssl req \
  -new \
  -nodes \
  -config src.txt \
  -keyout ${CERT_KEY_NAME}.key \
  -out ${CERT_KEY_NAME}.sr

echo Generate an OpenSSL Certificate for $CERT_KEY_NAME
openssl x509 -req \
  -in ${CERT_KEY_NAME}.sr \
  -extensions v3_req \
  -extfile src.txt \
  -CA customCA.crt -CAkey customCA.key \
  -CAcreateserial \
  -CAserial customCA.srl \
  -out $CERT_FILE \
  -passin file:passphrase.txt \
  -days 200

echo Generating PEM format
openssl rsa -in ${CERT_KEY_NAME}.key -out ${CERT_KEY_NAME}-nopassword.key
cat ${CERT_KEY_NAME}-nopassword.key > ${CERT_KEY_NAME}.pem
cat ${CERT_KEY_NAME}.crt >> ${CERT_KEY_NAME}.pem

echo Generating Bundle
cp $CERT_FILE $CERT_KEY_NAME-full.crt
cat customCA.crt >> $CERT_KEY_NAME-full.crt

echo Cleaning up temporary files
rm src.txt
rm ${CERT_KEY_NAME}.sr
rm ${CERT_KEY_NAME}-nopassword.key

echo DONE

