#!/bin/bash

# left in but needs to be looked at again if necessary

TIBERIUS_TEST_CONNECTION_STRING='server=tcp:localhost,1433;user=SA;password=<YourStrong@Passw0rd>;TrustServerCertificate=true' \
    cargo test