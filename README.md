# WavLM Audio Analysis - Test Setup

## Folder Structure
Create a folder structure like this for testing:

```
project_folder/
├── wavlm_analyzer.py          # Main code
├── sample_interview/          # Test interview folder
│   ├── reference_natural.wav  # Natural speaking reference
│   ├── reference_reading.wav  # Reading reference
│   ├── segment_1.wav          # Interview segment 1
│   ├── segment_2.wav          # Interview segment 2
│   └── segment_3.wav          # Interview segment 3
└── results/                   # Output folder (created automatically)
```

## How to Test

1. **Install dependencies:**
```bash
pip install torch torchaudio transformers soundfile numpy
```

2. **Prepare your audio files:**
   - Copy your existing audio files to the `sample_interview` folder
   - Rename them according to the structure above:
     - `reference_natural.wav` (your natural speaking sample)
     - `reference_reading.wav` (your reading sample)
     - `segment_1.wav`, `segment_2.wav`, etc. (your interview segments)

3. **Run the code:**
```bash
python wavlm_analyzer.py
```

## Expected Output

The script will:
1. Load the WavLM model
2. Process all audio files
3. Calculate similarities
4. Print results to console
5. Save results to a JSON file

## Sample Output Format

```json
{
  "interview_id": "interview_001",
  "final_verdict": "Cheating",
  "cheating_segments": 2,
  "segments": [
    {
      "segment_no": 1,
      "start_time": "00:00",
      "end_time": "01:00",
      "reading_cosine": 0.8900,
      "natural_cosine": 0.8500,
      "verdict": "Reading"
    },
    {
      "segment_no": 2,
      "start_time": "01:00",
      "end_time": "02:00",
      "reading_cosine": 0.6500,
      "natural_cosine": 0.9100,
      "verdict": "Natural"
    }
  ]
}
```

## Next Steps

Once this works locally, we can:
1. Create a FastAPI server wrapper
2. Add batch processing for multiple interviews
3. Add GCS integration
4. Deploy to cloud

Let me know if you encounter any issues with this setup!