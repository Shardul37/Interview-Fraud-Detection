import os

class Config:
    PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "your-gcp-project-id") # Replace with your actual project ID
    GCS_BUCKET_NAME = os.environ.get("GCS_BUCKET_NAME", "stag_metantz") # Your bucket name
    GCS_AUDIO_ROOT_PREFIX = os.environ.get("GCS_AUDIO_ROOT_PREFIX", "shardul_test/test_extracted_audio/") # Root prefix for interview audio folders

    # --- Other configurations ---
    # For local CPU processing, adjust based on your WAVLM analyzer's capabilities
    MAX_AUDIO_SEGMENTS_CPU = 1 
    MAX_AUDIO_SEGMENTS_GPU = 3 # For future GPU deployment: 2 reference + 3 segments = 5 total
    
    # Threshold for triggering GPU (number of interviews)
    GPU_TRIGGER_THRESHOLD = 3 # Example: trigger GPU after 3 interviews are ready

    # MongoDB connection (future)
    MONGO_URI = os.environ.get("MONGO_URI", "mongodb://localhost:27017/")
    MONGO_DB_NAME = os.environ.get("MONGO_DB_NAME", "ai_fraud_detection")
