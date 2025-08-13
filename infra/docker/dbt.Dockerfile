# dbt Postgres runner
FROM python:3.11-slim

WORKDIR /app

# Install system deps required by dbt (git is required for package management)
RUN apt-get update \
 && apt-get install -y --no-install-recommends git ca-certificates openssh-client \
 && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir \
      "dbt-postgres==1.7.8" \
      "psycopg2-binary" \
      "protobuf>=4.23,<5"

# Compose mounts the repo at /app so dbt sees your project + profiles
