Certificate generation
----------------------

In order to prepare the necessary self-signed certificates run the following commands

    ./generate-ca.sh
    ./generate-signed-cert.sh server

The first script creates a new signing-certificate, the second will then create new certificates with the given name, signed by the customCA.crt.
