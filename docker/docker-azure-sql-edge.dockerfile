FROM mcr.microsoft.com/azure-sql-edge:latest

USER root
COPY certs/server.* /certs/
RUN chmod 440 /certs/server.*
COPY certs/customCA.* /certs/
RUN chmod 440 /certs/customCA.*
COPY docker-mssql.conf /var/opt/mssql/mssql.conf
RUN chown mssql /var/opt/mssql/mssql.conf
USER mssql
