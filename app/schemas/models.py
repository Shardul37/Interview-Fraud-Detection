from pydantic import BaseModel
from typing import List, Dict, Any, Optional
from datetime import datetime
from enum import Enum

class ProcessingStatus(str, Enum):
    QUEUED = "QUEUED"
    VIDEO_DOWNLOADED = "VIDEO_DOWNLOADED" # New status for video conversion start
    AUDIO_EXTRACTED_QUEUED = "AUDIO_EXTRACTED_QUEUED"
    PROCESSING = "PROCESSING" # Generic processing stage, for video conversion or ML inference
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    NOT_FOUND = "NOT_FOUND"

class ProcessingHistoryEntry(BaseModel):
    timestamp: str
    status: ProcessingStatus
    stage: Optional[str] = None
    actor: str
    message: Optional[str] = None
    error: Optional[str] = None
    attempt: Optional[int] = None
    # Add any other relevant details for specific stages
    video_gcs_path: Optional[str] = None
    audio_gcs_prefix: Optional[str] = None
    batch_id: Optional[str] = None

#The SegmentResult and InterviewResult models are not used.
class SegmentResult(BaseModel):
    segment_no: int
    reading_cosine: float
    natural_cosine: float
    verdict: str
    processed_at: str

class InterviewResult(BaseModel):
    interview_id: str
    final_verdict: str
    cheating_segments: int
    total_segments: int
    processed_at: str
    processing_time_seconds: Optional[float] = None
    segments_details: List[SegmentResult]
    
    # NEW: Add history directly to InterviewResult model (reflects DB structure)
    history: List[ProcessingHistoryEntry] = []
