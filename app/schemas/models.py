from pydantic import BaseModel
from typing import List, Dict, Any, Optional
from datetime import datetime
from enum import Enum

# NEW: Enum for processing status
class ProcessingStatus(str, Enum):
    QUEUED = "QUEUED"
    AUDIO_EXTRACTED_QUEUED = "AUDIO_EXTRACTED_QUEUED" # Used by monitor
    PROCESSING = "PROCESSING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    NOT_FOUND = "NOT_FOUND" # For when an interview ID isn't found in GCS/DB

class SegmentResult(BaseModel):
    segment_no: int
    reading_cosine: float
    natural_cosine: float
    verdict: str
    processed_at: str # NEW: timestamp for each segment

class InterviewResult(BaseModel):
    interview_id: str
    final_verdict: str
    cheating_segments: int # integer
    total_segments: int
    json_file_path: Optional[str] = None # NEW: GCS path for results JSON
    embeddings_file_path: Optional[str] = None # NEW: GCS path for embeddings
    processed_at: str # timestamp for the whole interview processing
    processing_time_seconds: Optional[float] = None # NEW: Processing time
    segments_details: List[SegmentResult] # NEW: Renamed 'segments' to 'segments_details' for clarity

class ProcessingResponse(BaseModel):
    success: bool
    interview_id: str
    result: Optional[InterviewResult] = None # Make optional as could fail before result
    message: str
    processing_time: Optional[float] = None # Keep this, or remove if InterviewResult.processing_time_seconds is sufficient
    status: ProcessingStatus = ProcessingStatus.QUEUED # NEW: Add overall status