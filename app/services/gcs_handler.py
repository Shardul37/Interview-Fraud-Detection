# app/services/gcs_handler.py
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
        """
        Lists all blob names (file paths) within a given GCS prefix.
        Args:
            prefix: The GCS prefix (folder path, e.g., 'test_audio_files/shardul_test/interview_abc/').
        Returns:
            A list of blob names (relative paths within the bucket).
        """
        blobs = self.storage_client.list_blobs(self.bucket_name, prefix=prefix)
        file_names = [blob.name for blob in blobs if not blob.name.endswith('/')] # Exclude folders
        return file_names

    def download_file(self, gcs_file_path: str, local_destination_path: str):
        """
        Downloads a file from GCS to a specified local path.
        Args:
            gcs_file_path: The full GCS path of the file (e.g., 'test_audio_files/shardul_test/interview_abc/reference_natural.wav').
            local_destination_path: The local path where the file should be saved.
        """
        blob = self.bucket.blob(gcs_file_path)
        try:
            blob.download_to_filename(local_destination_path)
            # print(f"Downloaded {gcs_file_path} to {local_destination_path}") # Removed print for less verbosity
        except Exception as e:
            print(f"Error downloading {gcs_file_path}: {e}")
            raise

    def upload_json(self, gcs_file_path: str, data: Dict[str, Any]):
        """
        Uploads a dictionary as JSON to GCS.
        Args:
            gcs_file_path: The GCS path where the JSON file should be stored (e.g., 'test_results/interview_123.json').
            data: The dictionary to be serialized as JSON.
        """
        blob = self.bucket.blob(gcs_file_path)
        try:
            json_string = json.dumps(data, indent=2)
            blob.upload_from_string(json_string, content_type='application/json')
            print(f"Uploaded JSON to gs://{self.bucket_name}/{gcs_file_path}")
        except Exception as e:
            print(f"Error uploading JSON to {gcs_file_path}: {e}")
            raise
        
    def upload_file(self, local_file_path: str, gcs_file_path: str):
        """
        Uploads a file from local path to GCS.
        Args:
            local_file_path: The local path to the file to upload.
            gcs_file_path: The GCS path where the file should be stored (e.g., 'audio_segments/interview_123/segment_1.wav').
        """
        blob = self.bucket.blob(gcs_file_path)
        try:
            blob.upload_from_filename(local_file_path)
            print(f"Uploaded file to gs://{self.bucket_name}/{gcs_file_path}")
        except Exception as e:
            print(f"Error uploading file to {gcs_file_path}: {e}")
            raise

    def upload_numpy(self, gcs_file_path: str, data: Dict[str, Any]):
        """
        Uploads embeddings data as a numpy file to GCS.
        Args:
            gcs_file_path: The GCS path where the numpy file should be stored (e.g., 'test_embeddings/interview_123.npy').
            data: The embeddings data to be saved as numpy file.
        """
        blob = self.bucket.blob(gcs_file_path)
        try:
            with tempfile.NamedTemporaryFile() as temp_file:
                np.save(temp_file.name, data)
                temp_file.flush()
                blob.upload_from_filename(temp_file.name, content_type='application/octet-stream')
            print(f"Uploaded numpy file to gs://{self.bucket_name}/{gcs_file_path}")
        except Exception as e:
            print(f"Error uploading numpy file to {gcs_file_path}: {e}")
            raise

    def download_folder_to_local_directory(self, gcs_folder_prefix: str, local_destination_dir: str) -> List[str]:
        """
        Downloads all files from a specific GCS folder (prefix) to a specified local directory.
        Args:
            gcs_folder_prefix: The GCS prefix representing the folder (e.g., 'test_audio_files/shardul_test/interview_abc/').
                                Make sure it ends with a '/' if it's a folder.
            local_destination_dir: The local path where the files should be saved.
        Returns:
            A list of paths to the downloaded local files.
        """
        if not os.path.isdir(local_destination_dir):
            raise ValueError(f"Local destination directory does not exist or is not a directory: {local_destination_dir}")

        print(f"Downloading files from GCS prefix '{gcs_folder_prefix}' to local directory '{local_destination_dir}'")
        
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
        """
        Deletes all blobs (files) within a given GCS prefix (acting like a folder).
        Args:
            gcs_folder_prefix: The GCS prefix representing the folder to delete (e.g., 'test_extracted_audio/interview_abc/').
                                Make sure it ends with a '/' to ensure it only deletes contents *within* that 'folder'.
        """
        print(f"Attempting to delete files under GCS prefix: gs://{self.bucket_name}/{gcs_folder_prefix}")
        blobs_to_delete = self.storage_client.list_blobs(self.bucket_name, prefix=gcs_folder_prefix)
        
        delete_count = 0
        for blob in blobs_to_delete:
            if not blob.name.endswith('/'): 
                try:
                    blob.delete()
                    print(f"Deleted: gs://{self.bucket_name}/{blob.name}")
                    delete_count += 1
                except Exception as e:
                    print(f"Error deleting {blob.name}: {e}")
        print(f"Finished attempting to delete {delete_count} files under prefix {gcs_folder_prefix}.")