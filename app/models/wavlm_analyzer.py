import os
import json
import torch
import torchaudio
import soundfile as sf
import numpy as np
import torch.nn.functional as F
from transformers import WavLMModel, Wav2Vec2FeatureExtractor
from typing import List, Dict, Any, Optional, Tuple
import glob
from datetime import datetime
import gc
import time # Import time for measuring processing duration

class WavLMAudioAnalyzer:
    def __init__(self, force_cpu: bool = True): 
        """Initialize the WavLM model and feature extractor"""
        try:
            print("Loading WavLM model...")
            self.feature_extractor = Wav2Vec2FeatureExtractor.from_pretrained("microsoft/wavlm-base-plus")
            self.model = WavLMModel.from_pretrained("microsoft/wavlm-base-plus")
            
            # Device selection
            if force_cpu:
                self.device = "cpu"
            else:
                self.device = "cuda" if torch.cuda.is_available() else "cpu"
            
            self.model = self.model.to(self.device)
            self.model.eval() # Set model to evaluation mode
            print(f"Model loaded on {self.device}")
            
            # Check GPU memory if using CUDA
            if self.device == "cuda":
                gpu_memory = torch.cuda.get_device_properties(0).total_memory / (1024**3)
                print(f"GPU Memory: {gpu_memory:.1f} GB")
                
        except Exception as e:
            print(f"Error loading model: {str(e)}")
            raise
    
    # --- Existing methods (load_and_preprocess_audio, extract_embeddings, cosine_similarity) go here, unchanged ---
    def load_and_preprocess_audio(self, filepath: str) -> Optional[np.ndarray]:
        """Load and preprocess a single audio file"""
        try:
            waveform, sr = sf.read(filepath)
            if len(waveform.shape) > 1:
                waveform = waveform.mean(axis=1)
            
            if sr != 16000:
                waveform = torch.tensor(waveform, dtype=torch.float32)
                resampler = torchaudio.transforms.Resample(orig_freq=sr, new_freq=16000)
                waveform = resampler(waveform).numpy()
            else:
                waveform = waveform.astype(np.float32)
            
            return waveform
        except Exception as e:
            print(f"Error loading {filepath}: {str(e)}")
            return None
    
    def extract_embeddings(self, filepaths: List[str]) -> Tuple[torch.Tensor, List[str]]:
        """Extract embeddings for multiple audio files"""
        waveforms = []
        valid_files = []
        
        for filepath in filepaths:
            waveform = self.load_and_preprocess_audio(filepath)
            if waveform is not None:
                waveforms.append(waveform)
                valid_files.append(filepath)
        
        if not waveforms:
            raise ValueError("No valid audio files found")
        
        try:
            # Ensure inputs are processed on the correct device
            inputs = self.feature_extractor(waveforms, sampling_rate=16000, return_tensors="pt", padding=True)
            inputs = {k: v.to(self.device) for k, v in inputs.items()}
            
            with torch.no_grad():
                outputs = self.model(**inputs)
            
            embeddings = outputs.last_hidden_state.mean(dim=1)
            
            if self.device == "cuda":
                torch.cuda.empty_cache()
            
            return embeddings, valid_files
            
        except Exception as e:
            print(f"Error during embedding extraction: {str(e)}")
            raise
    
    def cosine_similarity(self, a: torch.Tensor, b: torch.Tensor) -> float:
        """Calculate cosine similarity between two embeddings"""
        return F.cosine_similarity(a.unsqueeze(0), b.unsqueeze(0)).item()
    
    # --- UPDATED process_interview METHOD ---
    def process_interview(self, interview_folder: str, interview_id: str) -> Tuple[Dict[str, Any], Dict[str, torch.Tensor]]:
        """Process interview and return analysis results and embeddings"""
        start_time = time.time() # Start timer for overall processing

        # Find reference files
        natural_file = os.path.join(interview_folder, "reference_natural.wav")
        reading_file = os.path.join(interview_folder, "reference_reading.wav")
        
        if not os.path.exists(natural_file) or not os.path.exists(reading_file):
            raise FileNotFoundError(f"Reference files not found in {interview_folder}. Expected: {natural_file}, {reading_file}")
        
        # Find segment files (ensure they are .wav and sorted by number)
        segment_files = sorted(glob.glob(os.path.join(interview_folder, "segment_*.wav")), 
                               key=lambda x: int(os.path.basename(x).replace("segment_", "").replace(".wav", "")))
        if not segment_files:
            raise FileNotFoundError(f"No segment files found in {interview_folder} matching 'segment_*.wav'")
        
        # Extract embeddings for reference files
        ref_embeddings, _ = self.extract_embeddings([natural_file, reading_file])
        natural_embedding = ref_embeddings[0]
        reading_embedding = ref_embeddings[1]
        
        # Extract embeddings for segment files
        # The segment_embeddings tensor will contain embeddings for all valid segments
        # valid_segment_files will contain the actual file paths corresponding to these embeddings
        segment_embeddings, valid_segment_files = self.extract_embeddings(segment_files)
        
        # Prepare embeddings data (move to CPU for storage, if not already)
        embeddings_data = {
            "interview_id": interview_id,
            "reference_natural": natural_embedding.cpu().numpy(), # Convert to NumPy for easier storage later
            "reference_reading": reading_embedding.cpu().numpy(), # Convert to NumPy
            "segments": {}
        }
        
        # Analyze segments and populate results
        segments_details_list: List[Dict[str, Any]] = [] # Conforms to List[SegmentResult]
        cheating_segments = 0
        total_segments_processed = 0
        
        # Zip segment_embeddings with valid_segment_files to ensure correct mapping
        # Loop through embeddings and their corresponding file paths
        for i, (segment_embedding, segment_file_path) in enumerate(zip(segment_embeddings, valid_segment_files)):
            total_segments_processed += 1 # Count valid segments
            
            natural_cosine = self.cosine_similarity(segment_embedding, natural_embedding)
            reading_cosine = self.cosine_similarity(segment_embedding, reading_embedding)
            
            verdict = "Natural" if natural_cosine > reading_cosine else "Reading"
            if verdict == "Reading":
                cheating_segments += 1
            
            # Extract segment number from filename (e.g., "segment_1.wav" -> 1)
            try:
                segment_filename = os.path.basename(segment_file_path)
                segment_no = int(segment_filename.replace("segment_", "").replace(".wav", ""))
            except ValueError:
                # Fallback if filename format is unexpected
                segment_no = i + 1 
                print(f"Warning: Could not parse segment number from {segment_filename}. Using index {segment_no}.")
            
            # Add segment embedding to embeddings_data (converted to NumPy)
            embeddings_data["segments"][f"segment_{segment_no}"] = segment_embedding.cpu().numpy()
            
            # Create SegmentResult-conforming dictionary
            segment_info = {
                "segment_no": segment_no,
                "reading_cosine": round(reading_cosine, 4),
                "natural_cosine": round(natural_cosine, 4),
                "verdict": verdict,
                "processed_at": datetime.now().isoformat() # Timestamp for each segment
            }
            segments_details_list.append(segment_info)
        
        # Sort segments_details_list by segment_no for consistent output
        segments_details_list.sort(key=lambda x: x["segment_no"])
        
        # Determine final verdict based on your company's fraud detection logic
        # For example, if any segment is "Reading", it's considered Cheating.
        # Or, if cheating_segments > a certain threshold/percentage.
        # Let's keep your current logic: if any cheating segment is found
        final_verdict = "Cheating" if cheating_segments > 0 else "Non-cheating"
        
        end_time = time.time() # End timer
        processing_duration_seconds = end_time - start_time

        # Construct the analysis_results dictionary to match InterviewResult schema
        analysis_results = {
            "interview_id": interview_id,
            "final_verdict": final_verdict,
            "cheating_segments": cheating_segments,
            "total_segments": total_segments_processed, # Use actual count of processed segments
            "json_file_path": None, # Will be filled in a later step when saving to GCS
            "embeddings_file_path": None, # Will be filled in a later step when saving to GCS
            "processed_at": datetime.now().isoformat(), # Timestamp for overall interview processing
            "processing_time_seconds": round(processing_duration_seconds, 2), # Total processing time
            "segments_details": segments_details_list # The list conforming to List[SegmentResult]
        }
        
        return analysis_results, embeddings_data
    
    def cleanup(self):
        """Clean up GPU memory"""
        if self.device == "cuda":
            torch.cuda.empty_cache()
            gc.collect()
