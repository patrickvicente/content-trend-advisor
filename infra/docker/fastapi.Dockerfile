FROM python:3.11-slim

WORKDIR /app

# Copy requirements first for better layer caching
COPY services/api/requirements.txt ./services/api/
RUN pip install --no-cache-dir -r ./services/api/requirements.txt

# Copy the entire project (since docker-compose mounts it anyway for dev)
COPY . ./

EXPOSE 8000

# Default command - docker-compose overrides this with --reload for dev
CMD ["uvicorn", "services.api.main:app", "--host", "0.0.0.0", "--port", "8000"]
