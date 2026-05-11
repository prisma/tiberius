FROM mcr.microsoft.com/mssql/server:2022-latest

USER root
COPY certs/server.* /certs/
RUN chmod 444 /certs/server.*
COPY certs/customCA.* /certs/
RUN chmod 444 /certs/customCA.*
COPY docker-mssql.conf /var/opt/mssql/mssql.conf
RUN chown mssql /var/opt/mssql/mssql.conf
USER mssql
