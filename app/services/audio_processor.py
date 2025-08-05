import os
import shutil
from typing import Optional


from app.models.wavlm_analyzer import WavLMAudioAnalyzer
from app.services.gcs_handler import GCSHandler
from app.services.mongodb_handler import MongoDBHandler
from config import Config
from app.schemas.models import ProcessingStatus

class AudioProcessorService:
    def __init__(self):
        self.analyzer = None
        self._load_model()
        self.gcs_handler = GCSHandler(Config.GCS_BUCKET_NAME)
        self.mongodb_handler = MongoDBHandler()

    def _load_model(self):
        try:
            print("Loading WavLM model...")
            self.analyzer = WavLMAudioAnalyzer(force_cpu=Config.FORCE_CPU_MODEL)
            print(f"Model loaded successfully on device: {self.analyzer.device}")
        except Exception as e:
            print(f"Error loading model: {str(e)}")
            raise

    async def process_single_interview_from_gcs(self, interview_id: str, batch_id: Optional[str] = None):
        print(f"--- Processing interview: {interview_id} ---")

        # Check MongoDB to see if this interview was already completed (idempotency)
        current_db_status = self.mongodb_handler.get_interview_status(interview_id)
        if current_db_status == ProcessingStatus.COMPLETED:
            print(f"Interview {interview_id} already COMPLETED in DB. Skipping.")
            # Record in history that it was skipped if needed for auditing
            self.mongodb_handler.add_history_entry(
                interview_id,
                status=ProcessingStatus.COMPLETED,
                stage="ml_inference",
                actor="ml_batch_processor",
                message="Skipped: Already completed.",
                batch_id=batch_id
            )
            return

        # Update status in MongoDB to 'PROCESSING' with history
        self.mongodb_handler.add_history_entry(
            interview_id,
            status=ProcessingStatus.PROCESSING,
            stage="ml_inference",
            actor="ml_batch_processor",
            message="Started ML inference.",
            batch_id=batch_id
        )

        interview_local_dir = None # Initialize to None for finally block
        try:
            gcs_interview_folder_prefix = f"{Config.GCS_AUDIO_ROOT_PREFIX}{interview_id}/"
            
            # Create a sub-directory for this interview's files
            interview_local_dir = os.path.join(Config.LOCAL_TEMP_AUDIO_SEGMENTS_DIR, interview_id)
            os.makedirs(interview_local_dir, exist_ok=True)
            
            # Download files for this specific interview to its sub-directory
            downloaded_files = self.gcs_handler.download_folder_to_local_directory(
                gcs_interview_folder_prefix,
                interview_local_dir
            )
            
            # --- Validate downloaded files ---
            ref_natural_exists = os.path.exists(os.path.join(interview_local_dir, Config.REFERENCE_NATURAL_FILE))
            ref_reading_exists = os.path.exists(os.path.join(interview_local_dir, Config.REFERENCE_READING_FILE))
            segment_files_count = len([f for f in os.listdir(interview_local_dir) if f.startswith(f"{Config.SEGMENT_FILE_PREFIX}") and f.endswith(".wav")])

            if not ref_natural_exists or not ref_reading_exists:
                raise ValueError(f"Missing reference audio files for interview {interview_id}.")
            if segment_files_count < Config.MIN_EXPECTED_INTERVIEW_SEGMENTS:
                raise ValueError(f"Not enough interview segments ({segment_files_count}) for interview {interview_id}. Expected at least {Config.MIN_EXPECTED_INTERVIEW_SEGMENTS}.")

            print(f"Downloaded {len(downloaded_files)} files for {interview_id}. Processing...")

            # Process the interview using WavLM Analyzer
            analysis_results = self.analyzer.process_interview(
                interview_local_dir, interview_id
            )
            
            # Upload embeddings to GCS if local embeddings directory exists
            local_embeddings_dir = analysis_results.get("local_embeddings_dir")
            embeddings_gcs_prefix = None
            
            if local_embeddings_dir and os.path.exists(local_embeddings_dir):
                embeddings_gcs_prefix = f"{Config.GCS_EMBEDDINGS_PREFIX}{interview_id}/"
                try:
                    uploaded_embedding_paths = self.gcs_handler.upload_folder_to_gcs(
                        local_embeddings_dir, embeddings_gcs_prefix
                    )
                    print(f"Uploaded {len(uploaded_embedding_paths)} embeddings to GCS for {interview_id}")
                except Exception as e:
                    print(f"Warning: Failed to upload embeddings for {interview_id}: {e}")
                    # Don't fail the entire process if embedding upload fails
            
            # Update MongoDB with results and COMPLETED status (including the full segments_details)
            # Include embeddings GCS path if available
            if embeddings_gcs_prefix:
                analysis_results["embeddings_gcs_prefix"] = embeddings_gcs_prefix
            
            self.mongodb_handler.store_processing_results(
                interview_id, analysis_results
            )
            
            print(f"Interview {interview_id} processed and results stored in MongoDB.")
            
            # Add final COMPLETED history entry
            self.mongodb_handler.add_history_entry(
                interview_id,
                status=ProcessingStatus.COMPLETED,
                stage="ml_inference",
                actor="ml_batch_processor",
                message="ML inference completed successfully.",
                batch_id=batch_id,
                processing_time_seconds=analysis_results.get("processing_time_seconds"),
                embeddings_gcs_prefix=embeddings_gcs_prefix
            )

        except Exception as e:
            error_message = f"Error processing interview {interview_id}: {str(e)}"
            print(error_message)
            self.mongodb_handler.add_history_entry(
                interview_id,
                status=ProcessingStatus.FAILED,
                stage="ml_inference",
                actor="ml_batch_processor",
                error=error_message,
                batch_id=batch_id
            )
            raise # Re-raise to signal failure to the caller (ml_batch_processor)

        finally:
            # Clean up the individual interview's local directory after processing
            if interview_local_dir and os.path.exists(interview_local_dir):
                shutil.rmtree(interview_local_dir)
                print(f"Cleaned up local audio directory: {interview_local_dir}")
            
            # Clean up the local embeddings directory after processing
            embeddings_dir = os.path.join(Config.LOCAL_TEMP_EMBEDDINGS_DIR, interview_id)
            if os.path.exists(embeddings_dir):
                shutil.rmtree(embeddings_dir)
                print(f"Cleaned up local embeddings directory: {embeddings_dir}")
    
    def is_model_loaded(self) -> bool:
        return self.analyzer is not None
    
    def get_device(self) -> str:
        if self.analyzer:
            return self.analyzer.device
        return "unknown"

    def cleanup(self):
        if self.analyzer:
            self.analyzer.cleanup()
        if self.mongodb_handler.client:
            self.mongodb_handler.client.close()
            print("MongoDB client closed.")