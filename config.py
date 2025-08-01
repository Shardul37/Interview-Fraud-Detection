import os
from dotenv import load_dotenv
# Load environment variables from .env file at the very start
#load_dotenv()

try:
    from secrets_loader import secrets_manager
    secrets_manager() # Call the function to populate os.environ
    print("Secrets loaded into environment by config.py import.")
except Exception as e:
    print(f"ERROR: Failed to load secrets during config.py import: {e}")

class Config:
    ENV = os.getenv("ENV", "stag")
    PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "your-gcp-project-id")
    
    GCS_BUCKET_NAME = os.environ.get("GCS_BUCKET_NAME", "stag_metantz")
    GCS_AUDIO_ROOT_PREFIX = os.environ.get("GCS_AUDIO_ROOT_PREFIX", "shardul_test/test_extracted_audio/")
    GCS_RESULTS_PREFIX = os.environ.get("GCS_RESULTS_PREFIX", "shardul_test/test_json_result/")
    GCS_EMBEDDINGS_PREFIX = os.environ.get("GCS_EMBEDDINGS_PREFIX", "shardul_test/test_embeddings/")

    # RabbitMQ Configurations
    RABBITMQ_HOST = os.environ.get("RABBITMQ_HOST", "localhost")
    RABBITMQ_PORT = int(os.environ.get("RABBITMQ_PORT", 5672))
    RABBITMQ_USER = os.environ.get("RABBITMQ_USER", "guest")
    RABBITMQ_PASS = os.environ.get("RABBITMQ_PASS", "guest")
    #These 4 varibles are no longer used
    RABBITMQ_PROCESSING_QUEUE = os.environ.get("RABBITMQ_PROCESSING_QUEUE", "INTERVIEW_PROCESSING_QUEUE_SHARDULTEST") # This is now RabbitMQ2 for ML
    RABBITMQ_VIDEO_READY_QUEUE = os.environ.get("RABBITMQ_VIDEO_READY_QUEUE", "VIDEO_READY_QUEUE_SHARDULTEST") # This is RabbitMQ1 for Video Converter

    # --- Video Conversion specific settings ---
    MIN_AUDIO_SEGMENT_LENGTH_MS = int(os.environ.get("MIN_AUDIO_SEGMENT_LENGTH_MS", 15000)) # 15 seconds
    SILENCE_THRESH_DB = int(os.environ.get("SILENCE_THRESH_DB", -40))
    MIN_SILENCE_LEN_S = float(os.environ.get("MIN_SILENCE_LEN_S", 3.0))

    LOCAL_TEMP_VIDEO_DIR = os.environ.get("LOCAL_TEMP_VIDEO_DIR", "/tmp/raw_videos")
    LOCAL_TEMP_AUDIO_SEGMENTS_DIR = os.environ.get("LOCAL_TEMP_AUDIO_SEGMENTS_DIR", "/tmp/extracted_audio")
    
    # MongoDB Configurations
    #MONGO_URI = os.environ.get("MONGO_URI", "mongodb://localhost:27017/")
     # RabbitMQ Configurations (now fetched from Secret Manager)
    # Assuming secrets_loader.py sets STAG_RABBITMQ_URL
    RABBITMQ_URL = os.environ.get(f"{ENV}_RABBITMQ_URL") # No default here; it *must* be set by secrets_loader
    if RABBITMQ_URL is None:
        raise ValueError(f"Environment variable {ENV}_RABBITMQ_URL not set. Secrets might not have loaded correctly.")

    # MongoDB Configurations - NOW READ DIRECTLY FROM OS.ENVIRON, EXPECTING secrets_loader TO HAVE SET THEM
    MONGO_URI = os.environ.get(f"{ENV}_DATABASE_URL") # No default here; it *must* be set by secrets_loader
    if MONGO_URI is None:
        raise ValueError(f"Environment variable {ENV}_DATABASE_URL not set. Secrets might not have loaded correctly.")
    MONGO_DB_NAME = os.environ.get("MONGO_DB_NAME", "Cheating-Results")
    MONGO_COLLECTION_INTERVIEWS = os.environ.get("MONGO_COLLECTION_INTERVIEWS", "Result")

    # Processing Variables
    # The MAX_INTERVIEW_SEGMENTS_PER_MODEL_PASS refers to the number of *interview segments*
    # that can be processed in one batch, *excluding* the 2 reference segments.
    # So if it's 3, the model's batch size will be 2 (refs) + 3 (segments) = 5.
    MAX_INTERVIEW_SEGMENTS_PER_MODEL_PASS = int(os.environ.get("MAX_INTERVIEW_SEGMENTS_PER_MODEL_PASS", 1))

    MIN_EXPECTED_INTERVIEW_SEGMENTS = int(os.environ.get("MIN_EXPECTED_INTERVIEW_SEGMENTS", 1))

    REFERENCE_NATURAL_FILE = "reference_natural.wav"
    REFERENCE_READING_FILE = "reference_reading.wav"
    SEGMENT_FILE_PREFIX = "segment_"

    # These are for the old QueueMonitor, but keeping them if other parts rely on Config.
    # Not directly used by the new ml_batch_processor in its current form.
    GPU_TRIGGER_THRESHOLD = int(os.environ.get("GPU_TRIGGER_THRESHOLD", 3))
    GPU_POLLING_INTERVAL = int(os.environ.get("GPU_POLLING_INTERVAL", 5))
    GPU_POLLING_TIMEOUT = int(os.environ.get("GPU_POLLING_TIMEOUT", 300))
