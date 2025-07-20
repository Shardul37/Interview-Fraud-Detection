from fastapi import APIRouter, HTTPException, UploadFile, File, Query
from typing import List, Dict, Any
import os
import tempfile
import shutil
from datetime import datetime

from app.services.audio_processor import AudioProcessorService
from app.schemas.models import ProcessingResponse, InterviewResult

router = APIRouter()

# Initialize the audio processor service
audio_service = AudioProcessorService()

# This endpoint is for uploading files directly (your original local test)
@router.post("/process-interview-upload", response_model=ProcessingResponse)
async def process_interview_upload(
    interview_id: str,
    reference_natural: UploadFile = File(...),
    reference_reading: UploadFile = File(...),
    segments: List[UploadFile] = File(...)
):
    """
    Process an interview with reference files and segments uploaded directly.
    """
    try:
        # Create temporary directory for processing
        with tempfile.TemporaryDirectory() as temp_dir:
            # Save uploaded files
            await save_uploaded_files(
                temp_dir, reference_natural, reference_reading, segments
            )
            
            # Process the interview (using the local folder path method)
            result = audio_service.process_interview_from_local_folder(
                temp_dir, interview_id
            )
            
            return ProcessingResponse(
                success=True,
                interview_id=interview_id,
                result=result,
                message="Interview processed successfully"
            )
            
    except Exception as e:
        raise HTTPException(
            status_code=500, 
            detail=f"Error processing interview from upload: {str(e)}"
        )

# This endpoint is for processing from a local folder path (your original local test)
@router.post("/process-interview-local-folder", response_model=ProcessingResponse)
async def process_interview_from_local_folder(
    interview_id: str,
    folder_path: str = Query(..., description="Absolute local path to the interview audio folder")
):
    """
    Process interview from a local folder path (for local development/testing).
    """
    try:
        if not os.path.exists(folder_path):
            raise HTTPException(
                status_code=404, 
                detail=f"Folder not found: {folder_path}"
            )
        
        result = audio_service.process_interview_from_local_folder(folder_path, interview_id)
        
        return ProcessingResponse(
            success=True,
            interview_id=interview_id,
            result=result,
            message="Interview processed successfully from local folder"
        )
        
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Error processing interview from local folder: {str(e)}"
        )

# NEW ENDPOINT: For fetching and processing from GCS
@router.post("/process-interview-gcs", response_model=ProcessingResponse)
async def process_interview_from_gcs(
    interview_id: str = Query(..., description="Unique ID of the interview, corresponds to GCS folder name.")
):
    """
    Process an interview by fetching audio files from a GCS folder.
    The GCS path is expected to be gs://<bucket_name>/test_audio_files/shardul_test/{interview_id}/
    """
    try:
        print(f"Received request to process interview_id: {interview_id} from GCS.")
        result = audio_service.process_interview_from_gcs(interview_id)
        
        return ProcessingResponse(
            success=True,
            interview_id=interview_id,
            result=result,
            message=f"Interview {interview_id} processed successfully from GCS."
        )
        
    except Exception as e:
        print(f"API Error: {str(e)}") # Log the actual error for debugging
        raise HTTPException(
            status_code=500,
            detail=f"Error processing interview from GCS: {str(e)}. Please check logs for details."
        )


@router.get("/status")
async def get_service_status():
    """
    Get service status and model information
    """
    return {
        "status": "running",
        "model_loaded": audio_service.is_model_loaded(),
        "device": audio_service.get_device(),
        "timestamp": datetime.now().isoformat()
    }

async def save_uploaded_files(
    temp_dir: str,
    reference_natural: UploadFile,
    reference_reading: UploadFile,
    segments: List[UploadFile]
) -> Dict[str, str]:
    """
    Save uploaded files to temporary directory
    """
    file_paths = {}
    
    # Save reference files
    natural_path = os.path.join(temp_dir, "reference_natural.wav")
    reading_path = os.path.join(temp_dir, "reference_reading.wav")
    
    with open(natural_path, "wb") as f:
        shutil.copyfileobj(reference_natural.file, f)
    file_paths["reference_natural"] = natural_path
    
    with open(reading_path, "wb") as f:
        shutil.copyfileobj(reference_reading.file, f)
    file_paths["reference_reading"] = reading_path
    
    # Save segment files
    segment_paths = []
    for i, segment in enumerate(segments, 1):
        segment_path = os.path.join(temp_dir, f"segment_{i}.wav")
        with open(segment_path, "wb") as f:
            shutil.copyfileobj(segment.file, f)
        segment_paths.append(segment_path)
    file_paths["segments"] = segment_paths # Store list of segment paths
    
    return file_paths
