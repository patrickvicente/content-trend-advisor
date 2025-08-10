import os
from fastapi import FastAPI

app = FastAPI(title="Content Trend Advisor API")

@app.get("/")
async def root():
    return {"message": "Content Trend Advisor API is running"}

@app.get("/health")
async def health_check():
    return {
        "status": "healthy",
        "mlflow": os.getenv("MLFLOW_TRACKING_URI"),
        "db": os.getenv("DATABASE_URL", "")[:40] + "..."}
        
