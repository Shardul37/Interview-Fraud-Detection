# app/main.py
from dotenv import load_dotenv
load_dotenv() 

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import uvicorn
from app.api.endpoints import router
from app.services.audio_processor import audio_service # Access the initialized service

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

@app.on_event("shutdown")
async def shutdown_event():
    """Cleanup resources on application shutdown."""
    print("Shutting down FastAPI app. Cleaning up resources...")
    audio_service.cleanup() # This will close MongoDB client too

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)