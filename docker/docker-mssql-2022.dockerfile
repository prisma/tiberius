FROM mcr.microsoft.com/mssql/server:2022-latest

COPY --chmod=444 certs/server.* /certs/
COPY --chmod=444 certs/customCA.* /certs/
COPY --chown=mssql docker-mssql.conf /var/opt/mssql/mssql.conf
