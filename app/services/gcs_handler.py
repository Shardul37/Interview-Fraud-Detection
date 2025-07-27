import os
import tempfile
import json
import numpy as np
from google.cloud import storage
from typing import List, Dict, Tuple, Any

class GCSHandler:
    def __init__(self, bucket_name: str):
        self.bucket_name = bucket_name
        self.storage_client = storage.Client()
        self.bucket = self.storage_client.bucket(self.bucket_name)

    def list_files_in_prefix(self, prefix: str) -> List[str]:
        blobs = self.storage_client.list_blobs(self.bucket_name, prefix=prefix)
        file_names = [blob.name for blob in blobs if not blob.name.endswith('/')]
        return file_names

    def download_file(self, gcs_file_path: str, local_destination_path: str):
        blob = self.bucket.blob(gcs_file_path)
        try:
            blob.download_to_filename(local_destination_path)
        except Exception as e:
            raise RuntimeError(f"Error downloading {gcs_file_path}: {e}")
    
    def upload_json(self, gcs_file_path: str, data: Dict[str, Any]):
        blob = self.bucket.blob(gcs_file_path)
        try:
            json_string = json.dumps(data, indent=2)
            blob.upload_from_string(json_string, content_type='application/json')
            print(f"Uploaded JSON to gs://{self.bucket_name}/{gcs_file_path}")
        except Exception as e:
            raise RuntimeError(f"Error uploading JSON to {gcs_file_path}: {e}")
            
    def upload_file(self, local_file_path: str, gcs_file_path: str):
        blob = self.bucket.blob(gcs_file_path)
        try:
            blob.upload_from_filename(local_file_path)
        except Exception as e:
            raise RuntimeError(f"Error uploading file to {gcs_file_path}: {e}")

    def upload_numpy(self, gcs_file_path: str, data: Dict[str, Any]):
        blob = self.bucket.blob(gcs_file_path)
        try:
            with tempfile.NamedTemporaryFile() as temp_file:
                np.save(temp_file.name, data)
                temp_file.flush()
                blob.upload_from_filename(temp_file.name, content_type='application/octet-stream')
        except Exception as e:
            raise RuntimeError(f"Error uploading numpy file to {gcs_file_path}: {e}")

    def download_folder_to_local_directory(self, gcs_folder_prefix: str, local_destination_dir: str) -> List[str]:
        if not os.path.isdir(local_destination_dir):
            raise ValueError(f"Local destination directory does not exist or is not a directory: {local_destination_dir}")

        blobs = self.storage_client.list_blobs(self.bucket_name, prefix=gcs_folder_prefix)
        
        downloaded_paths = []
        for blob in blobs:
            if blob.name.endswith('/'):
                continue

            local_file_name = os.path.basename(blob.name)
            local_file_path = os.path.join(local_destination_dir, local_file_name)
            
            self.download_file(blob.name, local_file_path)
            downloaded_paths.append(local_file_path)
            
        return downloaded_paths

    def delete_folder_by_prefix(self, gcs_folder_prefix: str):
        blobs_to_delete = self.storage_client.list_blobs(self.bucket_name, prefix=gcs_folder_prefix)
        
        for blob in blobs_to_delete:
            if not blob.name.endswith('/'): 
                try:
                    blob.delete()
                except Exception as e:
                    print(f"Error deleting {blob.name}: {e}")