import os
import torch
import tempfile
import shutil
from typing import Dict, Any, Tuple
from datetime import datetime

from app.models.wavlm_analyzer import WavLMAudioAnalyzer
from app.services.gcs_handler import GCSHandler
from config import Config

class AudioProcessorService:
    def __init__(self):
        """Initialize the audio processor service"""
        self.analyzer = None
        self._load_model()
        self.gcs_handler = GCSHandler(Config.GCS_BUCKET_NAME)

    def _load_model(self):
        """Load the WavLM model"""
        try:
            print("Loading WavLM model...")
            self.analyzer = WavLMAudioAnalyzer() 
            print(f"Model loaded successfully on device: {self.analyzer.device}")
        except Exception as e:
            print(f"Error loading model: {str(e)}")
            raise

    def process_interview_from_gcs(self, interview_id: str) -> Dict[str, Any]:
        """
        Process an interview by fetching audio files from GCS and returning results.
        
        Args:
            interview_id: Unique identifier for the interview (which is also the GCS folder name).
            
        Returns:
            Dictionary containing analysis results.
        """
        gcs_interview_folder_prefix = f"{Config.GCS_AUDIO_ROOT_PREFIX}{interview_id}/"
        print(f"DEBUG: Config.GCS_AUDIO_ROOT_PREFIX: '{Config.GCS_AUDIO_ROOT_PREFIX}'")
        print(f"Attempting to process interview {interview_id} from GCS prefix: {gcs_interview_folder_prefix}")

        # Use TemporaryDirectory context manager for automatic cleanup
        with tempfile.TemporaryDirectory() as temp_local_dir:
            try:
                # Pass the temporary local directory to the GCS handler
                # Now, files will be downloaded directly into temp_local_dir
                downloaded_files = self.gcs_handler.download_folder_to_local_directory(
                    gcs_interview_folder_prefix,
                    temp_local_dir # This is the key change!
                )
                
                if not downloaded_files:
                    raise FileNotFoundError(f"No audio files found for interview_id '{interview_id}' in GCS at '{gcs_interview_folder_prefix}'.")

                print(f"Downloaded {len(downloaded_files)} files. Processing interview {interview_id}...")

                # Now, call the analyzer with the local temporary folder path
                analysis_results, embeddings_data = self.analyzer.process_interview(
                    temp_local_dir, interview_id
                )
                
                print(f"Interview {interview_id} processed successfully.")
                return analysis_results
                
            except FileNotFoundError as e:
                print(f"Client Error: {str(e)}")
                raise # Re-raise to be caught by FastAPI HTTPException
            except Exception as e:
                print(f"Error processing interview {interview_id} from GCS: {str(e)}")
                raise
        # The temporary directory and its contents are automatically removed when exiting the 'with' block

    def process_interview_from_local_folder(self, folder_path: str, interview_id: str) -> Dict[str, Any]:
        """
        Process an interview from a local folder path.
        This method is kept for local file system testing or debugging.
        """
        try:
            print(f"Processing interview {interview_id} from local folder: {folder_path}")
            analysis_results, embeddings_data = self.analyzer.process_interview(
                folder_path, interview_id
            )
            print(f"Interview {interview_id} processed successfully from local folder.")
            return analysis_results
        except Exception as e:
            print(f"Error processing interview {interview_id} from local folder: {str(e)}")
            raise
    
    def is_model_loaded(self) -> bool:
        """Check if model is loaded"""
        return self.analyzer is not None

    def get_device(self) -> str:
        """Get the device the model is running on"""
        if self.analyzer:
            return self.analyzer.device
        return "unknown"

    def cleanup(self):
        """Clean up resources"""
        if self.analyzer:
            self.analyzer.cleanup()
