# app/api/endpoints.py
from fastapi import APIRouter, HTTPException, UploadFile, File, Query, BackgroundTasks
from typing import List, Dict, Any, Optional
import os
import tempfile
import shutil
from datetime import datetime

from app.services.audio_processor import AudioProcessorService
from app.schemas.models import ProcessingResponse, InterviewResult, ProcessingStatus # Import new enum
from app.services.mongodb_handler import MongoDBHandler # For direct status updates if needed
from config import Config

router = APIRouter()

# Initialize services at startup
audio_service = AudioProcessorService()
mongodb_handler = MongoDBHandler() # For direct interaction if needed outside of audio_service

# This endpoint is for uploading files directly (your original local test) - KEEP for specific test
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
        with tempfile.TemporaryDirectory() as temp_dir:
            file_paths = await save_uploaded_files(
                temp_dir, reference_natural, reference_reading, segments
            )
            
            # This will use the local folder processing logic
            analysis_result, _ = audio_service.analyzer.process_interview(
                temp_dir, interview_id
            )
            
            return ProcessingResponse(
                success=True,
                interview_id=interview_id,
                result=InterviewResult(**analysis_result), # Ensure it matches schema
                message="Interview processed successfully from upload",
                status=ProcessingStatus.COMPLETED
            )
            
    except Exception as e:
        raise HTTPException(
            status_code=500, 
            detail=f"Error processing interview from upload: {str(e)}"
        )

# This endpoint is for processing from a local folder path (your original local test) - KEEP for specific test
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
        
        analysis_result, _ = audio_service.analyzer.process_interview(folder_path, interview_id)
        
        return ProcessingResponse(
            success=True,
            interview_id=interview_id,
            result=InterviewResult(**analysis_result),
            message="Interview processed successfully from local folder",
            status=ProcessingStatus.COMPLETED
        )
        
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Error processing interview from local folder: {str(e)}"
        )


# NEW BATCH PROCESSING ENDPOINT FOR GPU SERVER
@router.post("/process-batch")
async def process_batch(
    interview_ids: List[str], # Expects a JSON body like ["id1", "id2", ...]
    background_tasks: BackgroundTasks # For running the heavy processing in background
):
    """
    Initiates processing of a batch of interviews fetched from GCS.
    This endpoint is designed to be called by the Queue Monitor/Dispatcher.
    The actual processing will run as a background task.
    """
    if not interview_ids:
        raise HTTPException(status_code=400, detail="No interview IDs provided for batch processing.")
    
    batch_id = f"batch_{datetime.now().isoformat().replace(':', '-')}_{len(interview_ids)}"
    print(f"Received request to process batch '{batch_id}' with {len(interview_ids)} interviews.")
    
    # Immediately update MongoDB for each interview to indicate it's part of a batch
    for interview_id in interview_ids:
        # We set status to PROCESSING here, it will be updated to FAILED/COMPLETED by audio_service
        mongodb_handler.update_interview_status(interview_id, ProcessingStatus.PROCESSING, batch_id=batch_id)

    # Run the heavy processing as a background task
    # This ensures the API immediately returns a 200 OK, preventing timeouts
    # while the model is downloading and processing the batch.
    background_tasks.add_task(audio_service.process_batch_from_gcs, interview_ids)

    return {
        "success": True,
        "message": f"Batch processing initiated for {len(interview_ids)} interviews.",
        "batch_id": batch_id,
        "status": "PROCESSING_INITIATED"
    }


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
    
    natural_path = os.path.join(temp_dir, Config.REFERENCE_NATURAL_FILE)
    reading_path = os.path.join(temp_dir, Config.REFERENCE_READING_FILE)
    
    with open(natural_path, "wb") as f:
        shutil.copyfileobj(reference_natural.file, f)
    file_paths["reference_natural"] = natural_path
    
    with open(reading_path, "wb") as f:
        shutil.copyfileobj(reference_reading.file, f)
    file_paths["reference_reading"] = reading_path
    
    segment_paths = []
    for i, segment in enumerate(segments, 1):
        segment_path = os.path.join(temp_dir, f"segment_{i}.wav")
        with open(segment_path, "wb") as f:
            shutil.copyfileobj(segment.file, f)
        segment_paths.append(segment_path)
    file_paths["segments"] = segment_paths
    
    return file_paths
