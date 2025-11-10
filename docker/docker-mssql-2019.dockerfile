FROM mcr.microsoft.com/mssql/server:2019-latest

COPY --chmod=440 docker/certs/server.* /certs/
COPY --chmod=440 docker/certs/customCA.* /certs/
COPY --chown=mssql docker/docker-mssql.conf /var/opt/mssql/mssql.conf