FROM mcr.microsoft.com/azure-sql-edge:latest

COPY --chmod=440 certs/server.* /certs/
COPY --chmod=440 certs/customCA.* /certs/
COPY --chown=mssql docker-mssql.conf /var/opt/mssql/mssql.conf
