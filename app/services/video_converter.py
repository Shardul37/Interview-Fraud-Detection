import subprocess
import re
import os
import shutil
import sys
from typing import Dict, Any, List, Tuple
from datetime import datetime
import time

from config import Config
from app.services.gcs_handler import GCSHandler # Assuming GCSHandler is in app/services/

class VideoConverterService:
    def __init__(self):
        self.gcs_handler = GCSHandler(Config.GCS_BUCKET_NAME)
        self._ensure_ffmpeg_installed()
        os.makedirs(Config.LOCAL_TEMP_VIDEO_DIR, exist_ok=True)
        os.makedirs(Config.LOCAL_TEMP_AUDIO_SEGMENTS_DIR, exist_ok=True)

    def _ensure_ffmpeg_installed(self):
        """Verifies if ffmpeg and ffprobe are installed and accessible."""
        print("Verifying ffmpeg and ffprobe installation...")
        try:
            subprocess.run(["ffmpeg", "-version"], check=True, capture_output=True)
            subprocess.run(["ffprobe", "-version"], check=True, capture_output=True)
            print("ffmpeg and ffprobe are installed.")
        except (subprocess.CalledProcessError, FileNotFoundError):
            print("Error: ffmpeg/ffprobe not found. Please install them and ensure they are in your PATH.")
            print("Download from: https://ffmpeg.org/download.html")
            sys.exit(1) # Exit if essential tools are missing
        print("-" * 30)

    def _get_video_duration_s(self, video_path: str) -> float:
        """Gets video duration in seconds using ffprobe."""
        try:
            duration_cmd = [
                "ffprobe", "-v", "error", "-show_entries", "format=duration",
                "-of", "default=noprint_wrappers=1:nokey=1", video_path
            ]
            duration = float(subprocess.run(duration_cmd, capture_output=True, text=True, check=True).stdout.strip())
            return duration
        except Exception as e:
            print(f"Error getting video duration with ffprobe for {video_path}: {e}")
            raise

    def _extract_audio_from_video(self, video_path: str, output_audio_path: str):
        """Extracts audio (e.g., to MP3) from a video file."""
        print(f"Extracting audio from '{video_path}' to '{output_audio_path}'...")
        command = [
            "ffmpeg",
            "-i", video_path,
            "-vn", # No video
            "-acodec", "libmp3lame", # MP3 audio codec
            "-q:a", "2", # VBR quality (2 is good, 0 is best)
            output_audio_path
        ]
        try:
            subprocess.run(command, check=True, capture_output=True, text=True)
            print("Audio extraction complete.")
        except subprocess.CalledProcessError as e:
            print(f"Error extracting audio: {e.stderr}")
            raise

    def _extract_segment_timestamps_ffmpeg(self, input_audio_path: str) -> List[Dict[str, int]]:
        """
        Extracts timestamps of non-silent segments from an audio file using ffmpeg.
        Segments are filtered to be >= Config.MIN_AUDIO_SEGMENT_LENGTH_MS.
        """
        if not os.path.exists(input_audio_path):
            raise FileNotFoundError(f"Input audio file not found at '{input_audio_path}' for segment extraction.")

        print(f"Detecting segments in '{input_audio_path}' with ffmpeg...")
        
        command = [
            "ffmpeg",
            "-i", input_audio_path,
            "-af", f"silencedetect=n={Config.SILENCE_THRESH_DB}dB:d={Config.MIN_SILENCE_LEN_S}",
            "-f", "null",
            "-" # Output to stdout, but logs go to stderr
        ]

        try:
            process = subprocess.run(
                command,
                capture_output=True,
                text=True,
                check=True
            )
            ffmpeg_output = process.stderr # ffmpeg logs to stderr
            
        except subprocess.CalledProcessError as e:
            print(f"Error running ffmpeg for silence detection: {e.stderr}")
            return []
        except FileNotFoundError:
            print("Error: ffmpeg not found for silence detection.")
            return []

        silence_start_pattern = re.compile(r"silence_start: (\d+\.?\d*)")
        silence_end_pattern = re.compile(r"silence_end: (\d+\.?\d*)")

        silence_starts = []
        silence_ends = []

        for line in ffmpeg_output.splitlines():
            if "silence_start" in line:
                match = silence_start_pattern.search(line)
                if match:
                    silence_starts.append(float(match.group(1)))
            elif "silence_end" in line:
                match = silence_end_pattern.search(line)
                if match:
                    silence_ends.append(float(match.group(1)))

        audio_duration_s = self._get_video_duration_s(input_audio_path) # Use audio duration

        segments_ms = []
        current_segment_start = 0.0

        # Create segments based on silence detection
        for i in range(len(silence_starts)):
            silence_start = silence_starts[i]
            silence_end = silence_ends[i] if i < len(silence_ends) else audio_duration_s # Handle case where silence_ends might be shorter

            # Add the non-silent segment before this silence
            if silence_start > current_segment_start:
                segments_ms.append({
                    "start": int(current_segment_start * 1000),
                    "end": int(silence_start * 1000)
                })
            current_segment_start = silence_end # Next segment starts after this silence ends

        # Add the last segment if it's not silent until the end
        if current_segment_start < audio_duration_s:
            segments_ms.append({
                "start": int(current_segment_start * 1000),
                "end": int(audio_duration_s * 1000)
            })

        # Filter segments by minimum length
        filtered_segments = []
        for seg in segments_ms:
            if (seg["end"] - seg["start"]) >= Config.MIN_AUDIO_SEGMENT_LENGTH_MS:
                filtered_segments.append(seg)
        
        print(f"Found {len(filtered_segments)} segments ≥ {Config.MIN_AUDIO_SEGMENT_LENGTH_MS//1000}s.")
        return filtered_segments

    def _split_audio_segment(self, input_audio_path: str, output_path: str, start_ms: int, end_ms: int):
        """Splits an audio file into a segment and converts to WAV."""
        duration_s = (end_ms - start_ms) / 1000.0
        
        command = [
            "ffmpeg",
            "-i", input_audio_path,
            "-ss", str(start_ms / 1000.0), # Start time in seconds
            "-t", str(duration_s),       # Duration in seconds
            "-acodec", "pcm_s16le",      # PCM 16-bit little-endian (WAV standard)
            "-ar", "16000",              # 16 kHz sample rate (WavLM expects this)
            "-ac", "1",                  # Mono audio
            output_path
        ]
        try:
            subprocess.run(command, check=True, capture_output=True, text=True)
            # print(f"Split segment from {start_ms}ms to {end_ms}ms, saved to {output_path}") # Suppress for less verbosity
        except subprocess.CalledProcessError as e:
            print(f"Error splitting audio segment to {output_path}: {e.stderr}")
            raise

    def convert_video_to_audio_segments(self, video_id: str, gcs_video_path: str) -> str:
        """
        Main function to download video, extract segments, and upload to GCS.
        Returns the GCS prefix where segments are stored.
        """
        local_video_path = os.path.join(Config.LOCAL_TEMP_VIDEO_DIR, f"{video_id}.mp4")
        local_extracted_audio_path = os.path.join(Config.LOCAL_TEMP_AUDIO_SEGMENTS_DIR, f"{video_id}_full.mp3")
        
        # Create a local directory for the segments of this interview
        local_segment_output_dir = os.path.join(Config.LOCAL_TEMP_AUDIO_SEGMENTS_DIR, video_id)
        os.makedirs(local_segment_output_dir, exist_ok=True)

        gcs_output_prefix = f"{Config.GCS_AUDIO_ROOT_PREFIX}{video_id}/" # Where segments will be uploaded

        try:
            # 1. Download MP4 video from GCS
            print(f"Downloading video '{gcs_video_path}' to '{local_video_path}'...")
            self.gcs_handler.download_file(gcs_video_path, local_video_path)

            # 2. Extract full audio from video (to MP3 for timestamp extraction)
            self._extract_audio_from_video(local_video_path, local_extracted_audio_path)

            # 3. Extract segment timestamps from the full audio
            segments_timestamps = self._extract_segment_timestamps_ffmpeg(local_extracted_audio_path)

            # 4. Split audio into segments and convert to WAV
            segment_count = 0
            uploaded_segment_paths = []

            # First two segments are reference_natural and reference_reading
            if len(segments_timestamps) >= 2:
                # Reference Natural
                ref_natural_segment = segments_timestamps[0]
                ref_natural_output_path = os.path.join(local_segment_output_dir, Config.REFERENCE_NATURAL_FILE)
                self._split_audio_segment(
                    local_extracted_audio_path, ref_natural_output_path,
                    ref_natural_segment["start"], ref_natural_segment["end"]
                )
                self.gcs_handler.upload_file(ref_natural_output_path, f"{gcs_output_prefix}{Config.REFERENCE_NATURAL_FILE}")
                uploaded_segment_paths.append(f"{gcs_output_prefix}{Config.REFERENCE_NATURAL_FILE}")

                # Reference Reading
                ref_reading_segment = segments_timestamps[1]
                ref_reading_output_path = os.path.join(local_segment_output_dir, Config.REFERENCE_READING_FILE)
                self._split_audio_segment(
                    local_extracted_audio_path, ref_reading_output_path,
                    ref_reading_segment["start"], ref_reading_segment["end"]
                )
                self.gcs_handler.upload_file(ref_reading_output_path, f"{gcs_output_prefix}{Config.REFERENCE_READING_FILE}")
                uploaded_segment_paths.append(f"{gcs_output_prefix}{Config.REFERENCE_READING_FILE}")

                # Remaining segments are interview segments
                for i, segment in enumerate(segments_timestamps[2:], 1):
                    segment_output_path = os.path.join(local_segment_output_dir, f"{Config.SEGMENT_FILE_PREFIX}{i}.wav")
                    self._split_audio_segment(
                        local_extracted_audio_path, segment_output_path,
                        segment["start"], segment["end"]
                    )
                    self.gcs_handler.upload_file(segment_output_path, f"{gcs_output_prefix}{Config.SEGMENT_FILE_PREFIX}{i}.wav")
                    uploaded_segment_paths.append(f"{gcs_output_prefix}{Config.SEGMENT_FILE_PREFIX}{i}.wav")
                    segment_count += 1
            elif len(segments_timestamps) == 1:
                # Handle case with only one segment (e.g., use it as natural, no reading)
                print("Warning: Only one segment found. Using it as reference_natural, no reference_reading.")
                ref_natural_segment = segments_timestamps[0]
                ref_natural_output_path = os.path.join(local_segment_output_dir, Config.REFERENCE_NATURAL_FILE)
                self._split_audio_segment(
                    local_extracted_audio_path, ref_natural_output_path,
                    ref_natural_segment["start"], ref_natural_segment["end"]
                )
                self.gcs_handler.upload_file(ref_natural_output_path, f"{gcs_output_prefix}{Config.REFERENCE_NATURAL_FILE}")
                uploaded_segment_paths.append(f"{gcs_output_prefix}{Config.REFERENCE_NATURAL_FILE}")
                # No reading reference, no interview segments
            else:
                print("Warning: No sufficient segments found for reference or interview. Uploading nothing.")
                return "" # Indicate no segments uploaded

            print(f"Successfully created and uploaded {len(uploaded_segment_paths)} audio segments for {video_id}.")
            return gcs_output_prefix # Return the GCS prefix where segments are stored

        except Exception as e:
            print(f"Error during video to audio conversion for {video_id}: {e}")
            raise
        finally:
            # Clean up local temporary files
            if os.path.exists(local_video_path):
                os.remove(local_video_path)
            if os.path.exists(local_extracted_audio_path):
                os.remove(local_extracted_audio_path)
            if os.path.exists(local_segment_output_dir):
                shutil.rmtree(local_segment_output_dir)
            print(f"Cleaned up local temp files for {video_id}.")