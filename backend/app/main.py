from fastapi import FastAPI

app = FastAPI(
    title="TUCHAT API",
    description="Backend API for the TUCHAT messaging platform.",
    version="0.1.0"
)


@app.get("/")
async def root():
    return {
        "message": "Welcome to TUCHAT!",
        "status": "online",
        "version": "0.1.0"
    }


@app.get("/health")
async def health_check():
    return {
        "status": "healthy"
    }
