## Local WavLM Processing System

Two services connected by RabbitMQ, with Google Cloud Storage (GCS) for audio, and MongoDB for results/history:
- `monitoring/video_converter_consumer.py`: converts a raw interview video (in GCS) into audio segments and queues work for ML service.
- `monitoring/ml_batch_processor.py`: runs WavLM inference on those audio segments and stores results in MongoDB.

There are two dummy RabbitMQ producers:
- `monitoring/simulate_video_ready_producer.py`: produces video-ready messages (dummy message for the video converter service. Just ensure that that video folder is actually present in the said location in GCS).
- `monitoring/dummy_producer_ml_test.py`: produces audio-ready messages (to test only the ML service. Just ensure that that video folder is actually present in the said location in GCS).


### Quickstart (Local)

1) Prerequisites
- Python 3.13
- FFmpeg and FFprobe in PATH (required by the video converter)
- RabbitMQ and MongoDB accessible (URIs are pulled from Secret Manager; see Secrets section)
- Google Cloud service account key file with access to Secret Manager and GCS (Keep keyfile.json in root directory)
- Install the requirements.txt (it has some extra packages though) OR install requirements.ml_hob.txt & requirements.video_converter.txt
-If running locally set these env variables:
MAX_INTERVIEW_SEGMENTS_PER_MODEL_PASS=1 && FORCE_CPU_MODEL=True (Forces to use CPU instead of GPU)


3) Install dependencies
- For the video converter service only:
pip install -r requirements.video_converter.txt

- For the ML service only:
pip install -r requirements.ml_job.txt


4) If there are no messages in the queues, run a dummy producer
- Produce video-ready messages (for the video converter):
python -m monitoring.simulate_video_ready_producer

5) Start the services in order
- First, start the video converter consumer (consumes video-ready messages and uploads audio segments to GCS):
python -m monitoring.video_converter_consumer

- Then, start the ML batch processor (consumes audio-ready messages and writes results to MongoDB):
python -m monitoring.ml_batch_processor

- If you want to test ML service directly (push a demmy message to queue, then run the ml_batch_processor):
python -m monitoring.dummy_producer_ml_test



Notes
- For Windows paths in messages, the consumer normalizes them. Messages must include `interview_id` and a GCS `path` for the video.
- The ML job is memory hungry; for CPU tests, keep `FORCE_CPU_MODEL=True`. For GPU, set it to `False` and run on a CUDA machine and keep this: MAX_INTERVIEW_SEGMENTS_PER_MODEL_PASS=1  (This tell how man audio segments we want to pass through the wavLM model. For 8GB ram you cannot use >1)

### Secrets (Google Secret Manager)

`secrets_manager.py` loads required secrets at import time via `config.py`:
- Required secrets (MUST exist in Secret Manager under your `ENV` prefix):
  - `${ENV}_DATABASE_URL` → `Config.MONGO_URI`
  - `${ENV}_RABBITMQ_URL` → `Config.RABBITMQ_URL`

How it works
- Locally, it initializes the Secret Manager client using the service account at `GOOGLE_APPLICATION_CREDENTIALS`.

Permissions
- The service account must have access to read those secrets and to read/write the GCS bucket.

### RabbitMQ dummy producers

- `monitoring/simulate_video_ready_producer.py` sends messages to `Config.RABBITMQ_VIDEO_READY_QUEUE` with payload:
  - `{ "interview_id": "...", "path": "<gcs/video/path.mp4>" }`
- `monitoring/dummy_producer_ml_test.py` sends messages to `Config.RABBITMQ_PROCESSING_QUEUE` with payload:
  - `{ "interview_id": "...", "gcs_audio_prefix": "<gcs/prefix/of/segments/>" }`

Use these when the queue is empty, or to test a specific service end-to-end.

### Docker

Build images
```
# Video converter
docker build -f Dockerfile.video_converter -t video-converter-service .

# ML batch processor (GPU-capable base image)
docker build -f Dockerfile.ml_job -t ml-job-service .
```
### How it works (high level)

Flow
1) Video-ready message arrives at `RABBITMQ_VIDEO_READY_QUEUE` (`interview_id`, `path`).
2) `VideoConverterConsumer` downloads the video from GCS using `GCSHandler`, extracts full audio using FFmpeg, splits into segments with PyDub, and uploads the WAV files to `GCS_AUDIO_ROOT_PREFIX/<interview_id>/`.
3) It publishes an audio-ready message to `RABBITMQ_PROCESSING_QUEUE` (`interview_id`, `gcs_audio_prefix`).
4) `ml_batch_processor` consumes that message, downloads the corresponding files, validates references (`reference_natural.wav`, `reference_reading.wav`) and segments, then runs WavLM inference in batches.
5) Results are written to MongoDB; embeddings are optionally uploaded to `GCS_EMBEDDINGS_PREFIX/<interview_id>/`.

Key modules
- `app/services/video_converter.py` → FFmpeg extraction + PyDub segmentation + GCS upload
- `app/services/audio_processor.py` → Orchestrates download, validation, model inference, results storage
- `app/models/wavlm_analyzer.py` → Loads Microsoft WavLM, extracts embeddings, computes cosine similarities, produces verdicts
- `app/services/gcs_handler.py` → Thin wrapper over GCS client
- `app/services/mongodb_handler.py` → CRUD into MongoDB collection, status + history updates, results storage
- `app/services/rabbitmq_client.py` → Publish/consume with reconnection, ack/nack handling
- `config.py` → Loads `.env`, then loads secrets via `secrets_manager.py`, exposes `Config`
- `monitoring/video_converter_consumer.py` → RabbitMQ consumer for video-ready
- `monitoring/ml_batch_processor.py` → Continuous ML queue poller/processor
- Producers: `monitoring/simulate_video_ready_producer.py`, `monitoring/dummy_producer_ml_test.py`

### MongoDB: results and history

Database and collection
- DB: `Config.MONGO_DB_NAME` (default: `Cheating-Results`)
- Collection: `Config.MONGO_COLLECTION_INTERVIEWS` (default: `Result`)

Document shape (representative)
```json
{
  "_id": "<interview_id>",
  "status": "COMPLETED",
  "processing_attempts": 2,
  "last_updated": "2025-01-01T12:34:56.789",
  "completed_at": "2025-01-01T12:35:10.123",
  "results": {
    "interview_id": "<interview_id>",
    "final_verdict": "Cheating|Non-cheating",
    "cheating_segments": 1,
    "total_segments": 5,
    "processing_time_seconds": 42.7,
    "segments_details": [
      {
        "segment_no": 1,
        "reading_cosine": 0.1234,
        "natural_cosine": 0.5678,
        "verdict": "Reading|Natural",
        "processed_at": "2025-01-01T12:35:00.000"
      }....
    ],
    "embeddings_gcs_prefix": "shardul_test/test_embeddings/<interview_id>/"
  },
  "history": [
    {
      "timestamp": "2025-01-01T12:30:00.000",
      "status": "PROCESSING",
      "stage": "video_conversion",
      "actor": "video_converter_consumer",
      "message": "Started video to audio conversion.",
      "video_gcs_path": "shardul_test/test_videos/foo.mp4"
    },
    {
      "timestamp": "2025-01-01T12:32:00.000",
      "status": "AUDIO_EXTRACTED_QUEUED",
      "stage": "video_conversion",
      "actor": "video_converter_consumer",
      "message": "Video conversion completed and audio segments uploaded.",
      "audio_gcs_prefix": "shardul_test/test_extracted_audio/<interview_id>/"
    },
    {
      "timestamp": "2025-01-01T12:34:00.000",
      "status": "PROCESSING",
      "stage": "ml_inference",
      "actor": "ml_batch_processor",
      "message": "Started ML inference."
    },
    {
      "timestamp": "2025-01-01T12:35:10.123",
      "status": "COMPLETED",
      "stage": "ml_inference",
      "actor": "ml_batch_processor",
      "message": "ML inference completed successfully.",
      "processing_time_seconds": 42.7,
      "embeddings_gcs_prefix": "shardul_test/test_embeddings/<interview_id>/"
    }
  ]
}
```

### Troubleshooting

- RabbitMQ URL/Mongo URI errors: ensure `${ENV}_RABBITMQ_URL` and `${ENV}_DATABASE_URL` exist in Secret Manager and your service account has access.
- GCS permissions: the service account needs read for raw videos, read/write for extracted audio and embeddings.
- FFmpeg not found: install FFmpeg and FFprobe, or run the video converter in its Docker image.
