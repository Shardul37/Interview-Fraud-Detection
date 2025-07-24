from pydub import AudioSegment
from pydub.silence import detect_nonsilent
import json

def extract_segment_timestamps(input_audio, min_segment_length=15000, silence_thresh=-40):
    """
    Returns timestamps (in ms) of speech segments ≥ min_segment_length.
    Output format: {"segments": [{"start": 1500, "end": 18000}, ...]}
    """
    # Load audio (faster for MP3/WAV)
    audio = AudioSegment.from_file(input_audio)  # Auto-detects MP3/WAV
    
    # Detect nonsilent segments
    nonsilent_segments = detect_nonsilent(
        audio,
        min_silence_len=3000,      # 3s silence = split point
        silence_thresh=silence_thresh
    )
    
    # Filter segments by length and convert to list of dicts
    segments = [
        {"start": start, "end": end}
        for start, end in nonsilent_segments
        if (end - start) >= min_segment_length
    ]
    
    # Save timestamps to JSON
    output = {"segments": segments}
    with open("segment_timestamps.json", "w") as f:
        json.dump(output, f, indent=2)
    
    print(f"Found {len(segments)} segments ≥ {min_segment_length//1000}s. Timestamps saved to JSON.")
    return output

# Example usage
timestamps = extract_segment_timestamps(
    "sample_interview/mp3audio_83.mp3",
    min_segment_length=15000,
    silence_thresh=-40
)