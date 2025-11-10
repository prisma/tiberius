FROM mcr.microsoft.com/mssql/server:2022-latest

COPY --chmod=444 docker/certs/server.* /certs/
COPY --chmod=444 docker/certs/customCA.* /certs/
COPY --chown=mssql docker/docker-mssql.conf /var/opt/mssql/mssql.conf