# app/services/mongodb_handler.py
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure, OperationFailure
from typing import Dict, Any, List, Optional
from datetime import datetime
from config import Config
from app.schemas.models import ProcessingStatus # Import your enum
import certifi

class MongoDBHandler:
    def __init__(self):
        self.client = None
        self.db = None
        self.collection = None
        self._connect()

    def _connect(self):
        """Establishes connection to MongoDB."""
        try:
            self.client = MongoClient(Config.MONGO_URI)
            # The ismaster command is cheap and does not require auth.
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

    def get_interview_status(self, interview_id: str) -> Optional[ProcessingStatus]:
        """Fetches the status of an interview."""
        try:
            doc = self.collection.find_one({"_id": interview_id}, {"status": 1})
            if doc:
                return ProcessingStatus(doc.get("status", ProcessingStatus.QUEUED))
            return None # Not found
        except Exception as e:
            print(f"Error getting status for {interview_id}: {e}")
            return None

    def update_interview_status(self, interview_id: str, status: ProcessingStatus, **kwargs):
        """Updates the status and other fields of an interview document."""
        update_data = {"$set": {"status": status.value, "last_updated": datetime.now().isoformat()}}
        if kwargs:
            update_data["$set"].update(kwargs)
        
        # Increment processing_attempts if status is PROCESSING or FAILED
        if status in [ProcessingStatus.PROCESSING, ProcessingStatus.FAILED]:
            update_data["$inc"] = {"processing_attempts": 1}
        
        try:
            result = self.collection.update_one(
                {"_id": interview_id},
                update_data,
                upsert=True # Create the document if it doesn't exist
            )
            if result.matched_count > 0:
                print(f"Updated interview {interview_id} status to {status.value}.")
            elif result.upserted_id:
                print(f"Created new interview entry for {interview_id} with status {status.value}.")
            return result
        except Exception as e:
            print(f"Error updating status for {interview_id} to {status.value}: {e}")
            raise

    def store_processing_results(self, interview_id: str, results: Dict[str, Any], embeddings_gcs_path: Optional[str] = None, json_gcs_path: Optional[str] = None):
        """Stores the full processing results for an interview."""
        update_data = {
            "results": results,
            "status": ProcessingStatus.COMPLETED.value,
            "embeddings_gcs_path": embeddings_gcs_path,
            "json_gcs_path": json_gcs_path,
            "completed_at": datetime.now().isoformat(),
            "last_updated": datetime.now().isoformat()
        }
        try:
            self.collection.update_one(
                {"_id": interview_id},
                {"$set": update_data},
                upsert=True
            )
            print(f"Stored processing results for interview {interview_id}.")
        except Exception as e:
            print(f"Error storing results for {interview_id}: {e}")
            raise

    def get_unprocessed_interviews_for_batch(self, limit: int) -> List[str]:
        """
        Fetches a list of interview_ids that are not yet COMPLETED or FAILED,
        suitable for forming a new batch.
        """
        try:
            # We look for documents that are either 'QUEUED', 'AUDIO_EXTRACTED_QUEUED', or 'FAILED'
            # and limit the result. Order by '_id' for consistent batching.
            # This is a simplified fetch; in a real system, you might add more complex filtering
            # like 'last_updated' for staleness, or a 'batch_id' to prevent picking up interviews
            # currently in a processing batch that haven't been acked yet.
            
            # For now, let's assume if it's not COMPLETED and not already PROCESSING, it's fair game.
            # This relies on the GPU app updating to PROCESSING promptly.
            cursor = self.collection.find(
                {"status": {"$in": [
                    ProcessingStatus.QUEUED.value,
                    ProcessingStatus.AUDIO_EXTRACTED_QUEUED.value,
                    ProcessingStatus.FAILED.value
                ]}},
                {"_id": 1} # Only fetch the ID
            ).limit(limit)
            
            interview_ids = [doc["_id"] for doc in cursor]
            print(f"Found {len(interview_ids)} unprocessed interviews for batch.")
            return interview_ids
        except Exception as e:
            print(f"Error fetching unprocessed interviews: {e}")
            return []