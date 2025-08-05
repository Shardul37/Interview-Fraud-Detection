from pymongo import MongoClient
from pymongo.errors import ConnectionFailure, OperationFailure
from typing import Dict, Any, Optional
from datetime import datetime
from config import Config
from app.schemas.models import ProcessingStatus, ProcessingHistoryEntry

class MongoDBHandler:
    def __init__(self):
        self.client = None
        self.db = None
        self.collection = None
        self._connect()
        self._ensure_indexes()

    def _connect(self):
        try:
            self.client = MongoClient(Config.MONGO_URI)
            self.client.admin.command('ismaster')
            self.db = self.client[Config.MONGO_DB_NAME]
            self.collection = self.db[Config.MONGO_COLLECTION_INTERVIEWS]
            print("MongoDB connection established.")
        except ConnectionFailure as e:
            print(f"MongoDB connection failed: {e}")
            raise
        except OperationFailure as e:
            print(f"MongoDB authentication or operation failed: {e}")
            raise

    def _ensure_indexes(self):
        # Create an index on the 'status' field for efficient queries
        self.collection.create_index("status")
        print("MongoDB index on 'status' ensured.")

    def get_interview_status(self, interview_id: str) -> Optional[ProcessingStatus]:
        try:
            doc = self.collection.find_one({"_id": interview_id}, {"status": 1})
            if doc:
                return ProcessingStatus(doc.get("status", ProcessingStatus.QUEUED))
            return None
        except Exception as e:
            print(f"Error getting status for {interview_id}: {e}")
            return None

    def add_history_entry(self, interview_id: str, status: ProcessingStatus, **kwargs):
        """
        Adds a new entry to the 'history' array and updates the main 'status' field.
        """
        history_entry = ProcessingHistoryEntry(
            timestamp=datetime.now().isoformat(),
            status=status,
            **kwargs
        ).model_dump(exclude_none=True) # Use model_dump for Pydantic V2+

        update_fields = {
            "status": status.value,
            "last_updated": datetime.now().isoformat()
        }
        
        # If status is PROCESSING or FAILED, increment processing_attempts
        if status in [ProcessingStatus.PROCESSING, ProcessingStatus.FAILED]:
            self.collection.update_one(
                {"_id": interview_id},
                {"$inc": {"processing_attempts": 1}},
                upsert=True
            )

        try:
            result = self.collection.update_one(
                {"_id": interview_id},
                {"$set": update_fields, "$push": {"history": history_entry}},
                upsert=True
            )
            if result.matched_count > 0:
                print(f"Updated interview {interview_id} status to {status.value} and added history.")
            elif result.upserted_id:
                print(f"Created new interview entry for {interview_id} with status {status.value} and history.")
            return result
        except Exception as e:
            print(f"Error updating status/history for {interview_id} to {status.value}: {e}")
            raise

    def store_processing_results(self, interview_id: str, results: Dict[str, Any]):
        """
        Stores the full processing results for an interview and marks it COMPLETED.
        This updates the 'results' field and adds a history entry.
        """
        # Ensure that the 'results' dict can be directly stored (no numpy arrays etc.)
        # The 'analysis_results' dict from WavLMAudioAnalyzer should be fine.
        
        # Remove embedding paths as they are not stored in GCS anymore
        results.pop("json_file_path", None)
        results.pop("embeddings_file_path", None)

        update_data = {
            "results": results, # Store the full analysis_results directly
            "status": ProcessingStatus.COMPLETED.value,
            "completed_at": datetime.now().isoformat(),
            "last_updated": datetime.now().isoformat()
        }
        
        try:
            # We use add_history_entry to update status and add history
            # Then we also update the 'results' field in a separate operation
            # This ensures history is pushed even if result update fails later
            self.collection.update_one(
                {"_id": interview_id},
                {"$set": update_data}
            )
            print(f"Stored processing results for interview {interview_id}.")
        except Exception as e:
            print(f"Error storing results for {interview_id}: {e}")
            raise

    """
    def get_unprocessed_interviews_for_batch(self, limit: int) -> List[str]:
        
        #Fetches a list of interview_ids that are in AUDIO_EXTRACTED_QUEUED or FAILED state,
        #suitable for forming a new batch for ML processing.
        
        try:
            cursor = self.collection.find(
                {"status": {"$in": [
                    ProcessingStatus.AUDIO_EXTRACTED_QUEUED.value,
                    ProcessingStatus.FAILED.value # Re-process failed ones
                ]}},
                {"_id": 1} # Only fetch the ID
            ).limit(limit)
            
            interview_ids = [doc["_id"] for doc in cursor]
            print(f"Found {len(interview_ids)} unprocessed interviews for batch.")
            return interview_ids
        except Exception as e:
            print(f"Error fetching unprocessed interviews: {e}")
            return []
    """

    def close(self): # Renamed from cleanup to close for clarity
        if self.client:
            self.client.close()
            print("MongoDB client closed.")