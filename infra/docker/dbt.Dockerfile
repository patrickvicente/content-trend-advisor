# dbt Postgres runner
FROM python:3.11-slim

WORKDIR /app
RUN pip install --no-cache-dir dbt-postgres==1.7.8 psycopg2-binary

# Compose mounts the repo at /app so dbt sees your project + profiles
