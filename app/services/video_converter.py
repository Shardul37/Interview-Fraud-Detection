import subprocess
import os
import shutil
import sys
from typing import Dict, List

from pydub import AudioSegment
from pydub.silence import detect_nonsilent

from config import Config
from app.services.gcs_handler import GCSHandler

class VideoConverterService:
    def __init__(self):
        self.gcs_handler = GCSHandler(Config.GCS_BUCKET_NAME)
        self._ensure_ffmpeg_installed()
        os.makedirs(Config.LOCAL_TEMP_VIDEO_DIR, exist_ok=True)
        os.makedirs(Config.LOCAL_TEMP_AUDIO_SEGMENTS_DIR, exist_ok=True)

    def _ensure_ffmpeg_installed(self):
        try:
            subprocess.run(["ffmpeg", "-version"], check=True, capture_output=True)
            subprocess.run(["ffprobe", "-version"], check=True, capture_output=True)
        except (subprocess.CalledProcessError, FileNotFoundError):
            print("Error: ffmpeg/ffprobe not found. Please install them and ensure they are in your PATH.")
            sys.exit(1)

    def _extract_audio_from_video(self, video_path: str, output_audio_path: str):
        command = [
            "ffmpeg",
            "-i", video_path,
            "-vn",
            "-acodec", "libmp3lame",
            "-q:a", "2",
            output_audio_path
        ]
        try:
            subprocess.run(command, check=True, capture_output=True, text=True)
        except subprocess.CalledProcessError as e:
            raise RuntimeError(f"Error extracting full audio from video: {e.stderr}")

    def _detect_and_split_segments_pydub(self, full_audio_path: str, output_dir: str) -> List[Dict[str, int]]:
        if not os.path.exists(full_audio_path):
            raise FileNotFoundError(f"Full audio file not found at '{full_audio_path}'.")

        try:
            audio = AudioSegment.from_file(full_audio_path)
            
            nonsilent_segments = detect_nonsilent(
                audio,
                min_silence_len=int(Config.MIN_SILENCE_LEN_S * 1000),
                silence_thresh=Config.SILENCE_THRESH_DB,
            )
            
            filtered_segments_data = []
            segment_idx = 0

            for i, (start_ms, end_ms) in enumerate(nonsilent_segments):
                if (end_ms - start_ms) >= Config.MIN_AUDIO_SEGMENT_LENGTH_MS:
                    segment = audio[start_ms:end_ms]
                    
                    if segment_idx == 0:
                        output_filename = Config.REFERENCE_NATURAL_FILE
                    elif segment_idx == 1:
                        output_filename = Config.REFERENCE_READING_FILE
                    else:
                        output_filename = f"{Config.SEGMENT_FILE_PREFIX}{segment_idx - 1}.wav"
                    
                    output_filepath = os.path.join(output_dir, output_filename)
                    
                    segment.export(output_filepath, format="wav", parameters=["-ac", "1", "-ar", "16000"])
                    
                    filtered_segments_data.append({
                        "start": start_ms, 
                        "end": end_ms, 
                        "filename": output_filename
                    })
                    segment_idx += 1
            
            return filtered_segments_data
            
        except Exception as e:
            raise RuntimeError(f"Error during pydub segment detection and splitting for {full_audio_path}: {e}")

    def convert_video_to_audio_segments(self, video_id: str, gcs_video_path: str) -> str:
        local_video_path = os.path.join(Config.LOCAL_TEMP_VIDEO_DIR, f"{video_id}.mp4")
        local_extracted_audio_path = os.path.join(Config.LOCAL_TEMP_AUDIO_SEGMENTS_DIR, f"{video_id}_full.mp3")
        
        local_segment_output_dir = os.path.join(Config.LOCAL_TEMP_AUDIO_SEGMENTS_DIR, video_id)
        os.makedirs(local_segment_output_dir, exist_ok=True)

        gcs_output_prefix = f"{Config.GCS_AUDIO_ROOT_PREFIX}{video_id}/"

        uploaded_segment_paths = []

        try:
            print(f"Downloading video '{gcs_video_path}' to '{local_video_path}'...")
            self.gcs_handler.download_file(gcs_video_path, local_video_path)

            print(f"Extracting full audio from '{local_video_path}'...")
            self._extract_audio_from_video(local_video_path, local_extracted_audio_path)

            print(f"Detecting and splitting segments for '{video_id}'...")
            segments_info = self._detect_and_split_segments_pydub(
                local_extracted_audio_path, local_segment_output_dir
            )

            if not segments_info or len(segments_info) < Config.MIN_EXPECTED_INTERVIEW_SEGMENTS:
                 print(f"Warning: Not enough segments found for {video_id}. Found {len(segments_info)}, expected at least {Config.MIN_EXPECTED_INTERVIEW_SEGMENTS}. Skipping upload.")
                 return ""
            
            print(f"Uploading {len(segments_info)} segments to GCS for {video_id}...")
            for segment_data in segments_info:
                local_filename = segment_data["filename"]
                local_filepath = os.path.join(local_segment_output_dir, local_filename)
                gcs_destination_path = f"{gcs_output_prefix}{local_filename}"
                
                self.gcs_handler.upload_file(local_filepath, gcs_destination_path)
                uploaded_segment_paths.append(gcs_destination_path)

            print(f"Successfully uploaded {len(uploaded_segment_paths)} audio segments for {video_id}.")
            return gcs_output_prefix

        except Exception as e:
            raise RuntimeError(f"Video conversion failed for {video_id}: {e}")
        finally:
            if os.path.exists(local_video_path):
                os.remove(local_video_path)
            if os.path.exists(local_extracted_audio_path):
                os.remove(local_extracted_audio_path)
            if os.path.exists(local_segment_output_dir):
                shutil.rmtree(local_segment_output_dir)
            print(f"Cleaned up local temp files for {video_id}.")