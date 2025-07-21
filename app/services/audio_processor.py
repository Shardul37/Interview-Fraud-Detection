# app/services/audio_processor.py
import os
import torch
import tempfile
import shutil
from typing import Dict, Any, Tuple, List
from datetime import datetime
import time # For timing processing

from app.models.wavlm_analyzer import WavLMAudioAnalyzer
from app.services.gcs_handler import GCSHandler
from app.services.mongodb_handler import MongoDBHandler # NEW IMPORT
from config import Config
from app.schemas.models import ProcessingStatus, InterviewResult # NEW IMPORT for enum

class AudioProcessorService:
    def __init__(self):
        """Initialize the audio processor service."""
        self.analyzer = None
        self._load_model() # Model loads on init
        self.gcs_handler = GCSHandler(Config.GCS_BUCKET_NAME)
        self.mongodb_handler = MongoDBHandler() # Initialize MongoDB Handler

    def _load_model(self):
        """Load the WavLM model."""
        try:
            print("Loading WavLM model...")
            self.analyzer = WavLMAudioAnalyzer() 
            print(f"Model loaded successfully on device: {self.analyzer.device}")
        except Exception as e:
            print(f"Error loading model: {str(e)}")
            raise

    # New method to process a batch of interview IDs
    async def process_batch_from_gcs(self, interview_ids: List[str]) -> Dict[str, Any]:
        """
        Processes a batch of interviews by fetching audio files from GCS.
        This is the main entry point for the GPU instance.
        """
        total_interviews_in_batch = len(interview_ids)
        print(f"Received batch of {total_interviews_in_batch} interviews for processing.")
        
        batch_start_time = time.time()
        processed_results = []
        
        # Use a single temporary directory for the entire batch
        with tempfile.TemporaryDirectory() as batch_local_dir:
            print(f"Created temporary batch directory: {batch_local_dir}")
            
            for i, interview_id in enumerate(interview_ids):
                print(f"--- Processing interview {i+1}/{total_interviews_in_batch}: {interview_id} ---")
                
                # Update status in MongoDB immediately to 'PROCESSING'
                self.mongodb_handler.update_interview_status(
                    interview_id, ProcessingStatus.PROCESSING
                )

                try:
                    # Check MongoDB to see if this interview was already completed in a previous attempt
                    # This makes the processing idempotent
                    current_status = self.mongodb_handler.get_interview_status(interview_id)
                    if current_status == ProcessingStatus.COMPLETED:
                        print(f"Interview {interview_id} already completed. Skipping.")
                        # Fetch existing result if needed, or just continue
                        processed_results.append({
                            "interview_id": interview_id,
                            "status": ProcessingStatus.COMPLETED.value,
                            "message": "Already processed."
                        })
                        continue # Skip to next interview in the batch

                    gcs_interview_folder_prefix = f"{Config.GCS_AUDIO_ROOT_PREFIX}{interview_id}/"
                    
                    # Create a sub-directory for each interview's files within the batch temp dir
                    interview_local_dir = os.path.join(batch_local_dir, interview_id)
                    os.makedirs(interview_local_dir, exist_ok=True) # Ensure it exists
                    
                    # Download files for this specific interview to its sub-directory
                    downloaded_files = self.gcs_handler.download_folder_to_local_directory(
                        gcs_interview_folder_prefix,
                        interview_local_dir
                    )
                    
                    # --- Validate downloaded files ---
                    ref_natural_exists = os.path.exists(os.path.join(interview_local_dir, Config.REFERENCE_NATURAL_FILE))
                    ref_reading_exists = os.path.exists(os.path.join(interview_local_dir, Config.REFERENCE_READING_FILE))
                    segment_files = sorted([f for f in os.listdir(interview_local_dir) if f.startswith("segment_") and f.endswith(".wav")])

                    if not ref_natural_exists or not ref_reading_exists:
                        raise ValueError(f"Missing reference audio files for interview {interview_id}.")
                    if len(segment_files) < Config.MIN_EXPECTED_INTERVIEW_SEGMENTS:
                        raise ValueError(f"Not enough interview segments ({len(segment_files)}) for interview {interview_id}. Expected at least {Config.MIN_EXPECTED_INTERVIEW_SEGMENTS}.")

                    print(f"Downloaded {len(downloaded_files)} files for {interview_id}. Processing...")

                    # Process the interview using WavLM Analyzer
                    analysis_results, embeddings_data = self.analyzer.process_interview(
                        interview_local_dir, interview_id
                    )
                    
                    # Store results to GCS (JSON, Embeddings) and MongoDB
                    json_gcs_path = f"{Config.GCS_BUCKET_NAME}/{Config.GCS_RESULTS_PREFIX}{interview_id}.json"
                    embeddings_gcs_path = f"{Config.GCS_BUCKET_NAME}/{Config.GCS_EMBEDDINGS_PREFIX}{interview_id}.npy"
                    
                    # This would involve serializing analysis_results to JSON and embeddings_data to .npy
                    # For now, let's just log and update MongoDB
                    # self.gcs_handler.upload_json(f"{Config.GCS_RESULTS_PREFIX}{interview_id}.json", analysis_results)
                    # self.gcs_handler.upload_numpy(f"{Config.GCS_EMBEDDINGS_PREFIX}{interview_id}.npy", embeddings_data)

                    # Update MongoDB with results and COMPLETED status
                    self.mongodb_handler.store_processing_results(
                        interview_id, analysis_results, embeddings_gcs_path, json_gcs_path
                    )
                    
                    print(f"Interview {interview_id} processed and results stored.")
                    processed_results.append({
                        "interview_id": interview_id,
                        "status": ProcessingStatus.COMPLETED.value,
                        "message": "Processed successfully",
                        "result": analysis_results
                    })

                except Exception as e:
                    error_message = f"Error processing interview {interview_id}: {str(e)}"
                    print(error_message)
                    self.mongodb_handler.update_interview_status(
                        interview_id, ProcessingStatus.FAILED, error_message=error_message
                    )
                    processed_results.append({
                        "interview_id": interview_id,
                        "status": ProcessingStatus.FAILED.value,
                        "message": error_message
                    })
                finally:
                    # Clean up the individual interview's local directory after processing
                    if os.path.exists(interview_local_dir):
                        shutil.rmtree(interview_local_dir)
                        print(f"Cleaned up local directory: {interview_local_dir}")

        batch_end_time = time.time()
        batch_processing_duration = batch_end_time - batch_start_time
        print(f"--- Batch processing completed in {batch_processing_duration:.2f} seconds ---")
        
        return {
            "success": True,
            "total_interviews_in_batch": total_interviews_in_batch,
            "batch_processing_time_seconds": batch_processing_duration,
            "processed_details": processed_results
        }

    # Removed the old process_interview_from_gcs as batch processing is the new standard
    # You can keep process_interview_from_local_folder if needed for specific local tests

    def is_model_loaded(self) -> bool:
        """Check if model is loaded"""
        return self.analyzer is not None
    
    def get_device(self) -> str:
        """Get the device the model is running on"""
        if self.analyzer:
            return self.analyzer.device
        return "unknown"

    def cleanup(self):
        """Clean up resources (called on FastAPI shutdown)"""
        if self.analyzer:
            self.analyzer.cleanup()
        if self.mongodb_handler.client:
            self.mongodb_handler.client.close()
            print("MongoDB client closed.")
            
    
audio_service = AudioProcessorService()