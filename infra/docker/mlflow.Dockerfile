# MLflow server with minimal deps; artifacts go to MinIO via S3 protocol
FROM python:3.11-slim

WORKDIR /app
RUN pip install --no-cache-dir mlflow boto3

# We'll store the lightweight backend DB file in /app (sqlite) by default
# Artifacts go to s3://content/mlflow (MinIO)
EXPOSE 5000