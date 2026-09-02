FROM mcr.microsoft.com/mssql/server:2019-latest

USER root
COPY certs/server.* /certs/
RUN chmod 440 /certs/server.*
COPY certs/customCA.* /certs/
RUN chmod 440 /certs/customCA.*
COPY --chown=mssql docker-mssql.conf /var/opt/mssql/mssql.conf
USER mssql
