FROM mcr.microsoft.com/mssql/server:2019-latest

COPY --chmod=440 mssql.crt /var/opt/mssql/server.crt
COPY --chmod=440 mssql.key /var/opt/mssql/server.key
COPY --chown=mssql docker/docker-mssql.conf /var/opt/mssql/mssql.conf