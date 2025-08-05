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
import time

from config import Config

class WavLMAudioAnalyzer:
    def __init__(self, force_cpu: bool):
        try:
            print("Loading WavLM model...")
            self.feature_extractor = Wav2Vec2FeatureExtractor.from_pretrained("microsoft/wavlm-base-plus")
            self.model = WavLMModel.from_pretrained("microsoft/wavlm-base-plus")
            
            if force_cpu:
                self.device = "cpu"
            else:
                self.device = "cuda" if torch.cuda.is_available() else "cpu"
            
            self.model = self.model.to(self.device)
            self.model.eval()
            print(f"Model loaded on {self.device}")
            
            if self.device == "cuda":
                gpu_memory = torch.cuda.get_device_properties(0).total_memory / (1024**3)
                print(f"GPU Memory: {gpu_memory:.1f} GB")
                
        except Exception as e:
            print(f"Error loading model: {str(e)}")
            raise
    
    def load_and_preprocess_audio(self, filepath: str) -> Optional[np.ndarray]:
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
    
    def extract_embeddings(self, filepaths: List[str], save_to_dir: Optional[str] = None) -> Tuple[torch.Tensor, List[str]]:
        waveforms = []
        valid_files = []
        
        for filepath in filepaths:
            waveform = self.load_and_preprocess_audio(filepath)
            if waveform is not None:
                waveforms.append(waveform)
                valid_files.append(filepath)
        
        if not waveforms:
            raise ValueError("No valid audio files found for embedding extraction.")
        
        try:
            inputs = self.feature_extractor(waveforms, sampling_rate=16000, return_tensors="pt", padding=True)
            inputs = {k: v.to(self.device) for k, v in inputs.items()}
            
            with torch.no_grad():
                outputs = self.model(**inputs)
            
            embeddings = outputs.last_hidden_state.mean(dim=1)
            
            # Save embeddings locally if save_to_dir is provided
            if save_to_dir:
                os.makedirs(save_to_dir, exist_ok=True)
                for i, filepath in enumerate(valid_files):
                    filename = os.path.basename(filepath)
                    # Convert .wav to .pt for embedding filename
                    embedding_filename = filename.replace('.wav', '.pt')
                    embedding_path = os.path.join(save_to_dir, embedding_filename)
                    # Save individual embedding as .pt file
                    torch.save(embeddings[i].cpu(), embedding_path)
                    print(f"Saved embedding: {embedding_path}")
            
            if self.device == "cuda":
                torch.cuda.empty_cache()
            
            return embeddings, valid_files
            
        except Exception as e:
            print(f"Error during embedding extraction: {str(e)}")
            raise
    
    def cosine_similarity(self, a: torch.Tensor, b: torch.Tensor) -> float:
        return F.cosine_similarity(a.unsqueeze(0), b.unsqueeze(0)).item()
    
    def process_interview(self, interview_folder: str, interview_id: str) -> Dict[str, Any]:
        start_time = time.time()

        natural_file = os.path.join(interview_folder, Config.REFERENCE_NATURAL_FILE)
        reading_file = os.path.join(interview_folder, Config.REFERENCE_READING_FILE)
        
        if not os.path.exists(natural_file) or not os.path.exists(reading_file):
            raise FileNotFoundError(f"Reference files not found in {interview_folder}. Expected: {natural_file}, {reading_file}")
        
        segment_files = sorted(glob.glob(os.path.join(interview_folder, f"{Config.SEGMENT_FILE_PREFIX}*.wav")), 
                               key=lambda x: int(os.path.basename(x).replace(Config.SEGMENT_FILE_PREFIX, "").replace(".wav", "")))
        if not segment_files:
            print(f"No regular segment files found in {interview_folder}. Only processing references.")
        
        # Create embeddings directory for this interview
        embeddings_dir = os.path.join(Config.LOCAL_TEMP_EMBEDDINGS_DIR, interview_id)
        
        # Extract embeddings for reference files
        ref_embeddings, _ = self.extract_embeddings([natural_file, reading_file])
        natural_embedding = ref_embeddings[0]
        reading_embedding = ref_embeddings[1]
        
        segments_details_list: List[Dict[str, Any]] = []
        cheating_segments_count = 0
        total_segments_processed = 0

        all_audio_files_to_process = []
        # Add reference embeddings to a list for batch processing
        # We process segment_files in batches of MAX_INTERVIEW_SEGMENTS_PER_MODEL_PASS
        
        # Prepare list of segments including references for batching
        # Note: The segments should be mapped to their original segment_no for reporting later.
        
        # Collect all segment file paths and their original segment_no for proper ordering in reporting
        # This will be done per-interview and then iterated for batching for model inference
        
        # Process regular segments in batches (2 refs + MAX_INTERVIEW_SEGMENTS_PER_MODEL_PASS)
        for i in range(0, len(segment_files), Config.MAX_INTERVIEW_SEGMENTS_PER_MODEL_PASS):
            current_segment_batch_files = segment_files[i : i + Config.MAX_INTERVIEW_SEGMENTS_PER_MODEL_PASS]
            
            # Combine references with current batch of interview segments for model inference
            files_for_batch_inference = [natural_file, reading_file] + current_segment_batch_files
            
            # Extract embeddings for the current batch and save them locally
            batch_embeddings, batch_valid_files = self.extract_embeddings(files_for_batch_inference, save_to_dir=embeddings_dir)
            
            # The first two embeddings are always natural and reading references from this batch
            # We already have their embeddings extracted earlier, this re-extraction is okay for small batches
            # but for larger batches, you'd only extract references once and pass them down.
            # For simplicity and small batch sizes (2+3), current logic is fine.
            
            # Process results for the current batch of interview segments
            for j, segment_file_path in enumerate(current_segment_batch_files):
                # j is the index within current_segment_batch_files
                # The embedding for this segment is at index (2 + j) in batch_embeddings
                segment_embedding = batch_embeddings[2 + j] 

                total_segments_processed += 1
                
                natural_cosine = self.cosine_similarity(segment_embedding, natural_embedding)
                reading_cosine = self.cosine_similarity(segment_embedding, reading_embedding)
                
                verdict = "Natural" if natural_cosine > reading_cosine else "Reading"
                if verdict == "Reading":
                    cheating_segments_count += 1
                
                try:
                    segment_filename = os.path.basename(segment_file_path)
                    segment_no = int(segment_filename.replace(Config.SEGMENT_FILE_PREFIX, "").replace(".wav", ""))
                except ValueError:
                    segment_no = total_segments_processed # Fallback if filename isn't parseable
                    print(f"Warning: Could not parse segment number from {segment_filename}. Using index {segment_no}.")
                
                segment_info = {
                    "segment_no": segment_no,
                    "reading_cosine": round(reading_cosine, 4),
                    "natural_cosine": round(natural_cosine, 4),
                    "verdict": verdict,
                    "processed_at": datetime.now().isoformat()
                }
                segments_details_list.append(segment_info)
        
        segments_details_list.sort(key=lambda x: x["segment_no"])
        
        final_verdict = "Cheating" if cheating_segments_count > 0 else "Non-cheating"
        
        end_time = time.time()
        processing_duration_seconds = end_time - start_time

        analysis_results = {
            "interview_id": interview_id,
            "final_verdict": final_verdict,
            "cheating_segments": cheating_segments_count,
            "total_segments": total_segments_processed,
            # Removed file_paths as per new design
            "processed_at": datetime.now().isoformat(),
            "processing_time_seconds": round(processing_duration_seconds, 2),
            "segments_details": segments_details_list,
            "local_embeddings_dir": embeddings_dir
        }
        
        return analysis_results
    
    def cleanup(self):
        if self.device == "cuda":
            torch.cuda.empty_cache()
            gc.collect()
