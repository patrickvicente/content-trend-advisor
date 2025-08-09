FROM postgres:14

COPY infra/scripts/init_db.sql /docker-entrypoint-initdb.d/init_db.sql