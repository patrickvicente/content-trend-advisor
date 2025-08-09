from fastapi import FastAPI

app = FastAPI(title="Content Trend Advisor API")

@app.get("/")
async def root():
    return {"message": "Content Trend Advisor API is running"}

@app.get("/health")
async def health_check():
    return {"status": "healthy"}
