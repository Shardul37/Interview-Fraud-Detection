# NEW: Import dotenv and call load_dotenv() at the very beginning
from dotenv import load_dotenv
load_dotenv() # This must be called BEFORE any other imports that rely on env vars

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import uvicorn
from app.api.endpoints import router

# NEW: Import dotenv for loading .env file in local development
from dotenv import load_dotenv
load_dotenv() # This loads environment variables from a .env file if it exists

app = FastAPI(
    title="AI Fraud Detection API",
    description="Audio analysis API for detecting interview fraud",
    version="1.0.0"
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include API routes
app.include_router(router, prefix="/api/v1")

@app.get("/")
async def root():
    return {"message": "AI Fraud Detection API is running"}

@app.get("/health")
async def health_check():
    return {"status": "healthy"}

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)