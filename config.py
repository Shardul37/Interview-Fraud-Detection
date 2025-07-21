import os
from dotenv import load_dotenv
# Load environment variables from .env file at the very start
load_dotenv()

class Config:
    PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "your-gcp-project-id") # Replace with your actual project ID
    GCS_BUCKET_NAME = os.environ.get("GCS_BUCKET_NAME", "stag_metantz") # Your bucket name
    GCS_AUDIO_ROOT_PREFIX = os.environ.get("GCS_AUDIO_ROOT_PREFIX", "shardul_test/test_extracted_audio/") # Root prefix for interview audio folders
    GCS_RESULTS_PREFIX = os.environ.get("GCS_RESULTS_PREFIX", "test_results/")
    GCS_EMBEDDINGS_PREFIX = os.environ.get("GCS_EMBEDDINGS_PREFIX", "test_embeddings/")

    # RabbitMQ Configurations
    RABBITMQ_HOST = os.environ.get("RABBITMQ_HOST", "localhost")
    RABBITMQ_PORT = int(os.environ.get("RABBITMQ_PORT", 5672))
    RABBITMQ_USER = os.environ.get("RABBITMQ_USER", "guest")
    RABBITMQ_PASS = os.environ.get("RABBITMQ_PASS", "guest")
    RABBITMQ_PROCESSING_QUEUE = os.environ.get("RABBITMQ_PROCESSING_QUEUE", "interview_processing_queue")


    # MongoDB Configurations
    MONGO_URI = os.environ.get("MONGO_URI", "mongodb://localhost:27017/")
    MONGO_DB_NAME = os.environ.get("MONGO_DB_NAME", "ai_fraud_detection")
    MONGO_COLLECTION_INTERVIEWS = os.environ.get("MONGO_COLLECTION_INTERVIEWS", "interviews")

    # Processing Thresholds and Limits
    GPU_TRIGGER_THRESHOLD = int(os.environ.get("GPU_TRIGGER_THRESHOLD", 3)) # Number of interviews to accumulate before triggering GPU
    
    # Max audio segments to process in one model pass (2 references + X interview segments)
    # This is for the `_process_batch_and_get_results` method in WavLMAudioAnalyzer
    # e.g., if model takes 5 files total, and 2 are refs, then max_interview_segments_per_pass = 3
    MAX_INTERVIEW_SEGMENTS_PER_MODEL_PASS = int(os.environ.get("MAX_INTERVIEW_SEGMENTS_PER_MODEL_PASS", 3)) 

    # Minimum number of segment files expected to consider an interview "complete" for processing
    # This is for the GCS Queue Monitor's completeness check
    MIN_EXPECTED_INTERVIEW_SEGMENTS = int(os.environ.get("MIN_EXPECTED_INTERVIEW_SEGMENTS", 1))

    # Expected names for reference files
    REFERENCE_NATURAL_FILE = "reference_natural.wav"
    REFERENCE_READING_FILE = "reference_reading.wav"

    # Time to wait between polling for GPU readiness (in seconds)
    GPU_POLLING_INTERVAL = int(os.environ.get("GPU_POLLING_INTERVAL", 5))
    GPU_POLLING_TIMEOUT = int(os.environ.get("GPU_POLLING_TIMEOUT", 300)) # 5 minutes
