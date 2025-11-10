# FROM mcr.microsoft.com/mssql/server:2017-latest

# COPY --chmod=440 certs/server.* /certs/
# COPY --chmod=440 certs/customCA.* /certs/
# COPY docker-mssql.conf /var/opt/mssql/mssql.conf

FROM mcr.microsoft.com/azure-sql-edge:latest

COPY --chmod=440 docker/certs/server.* /certs/
COPY --chmod=440 docker/certs/customCA.* /certs/
COPY --chown=mssql docker/docker-mssql.conf /var/opt/mssql/mssql.conf