# monitoring/queue_monitor.py
import pika
import json
import time
import requests # For making HTTP calls to FastAPI
from typing import List, Dict, Any
from datetime import datetime
from threading import Thread, Lock # For thread safety if needed for state

from config import Config
from app.services.gcs_handler import GCSHandler
from app.services.mongodb_handler import MongoDBHandler # For database status
from app.services.rabbitmq_client import RabbitMQClient # For consuming messages
from app.schemas.models import ProcessingStatus # For status updates

class QueueMonitor:
    def __init__(self):
        self.rabbitmq_client = RabbitMQClient(
            host=Config.RABBITMQ_HOST,
            port=Config.RABBITMQ_PORT,
            user=Config.RABBITMQ_USER,
            password=Config.RABBITMQ_PASS
        )
        self.gcs_handler = GCSHandler(Config.GCS_BUCKET_NAME)
        self.mongodb_handler = MongoDBHandler()

        self.processing_queue = Config.RABBITMQ_PROCESSING_QUEUE
        self.gpu_trigger_threshold = Config.GPU_TRIGGER_THRESHOLD

        # In-memory store for messages received from RabbitMQ, along with their delivery tags
        # Format: [{"interview_id": "...", "delivery_tag": "...", "status_in_db": "..."}]
        self.unprocessed_batch_messages: List[Dict[str, Any]] = []
        self.unprocessed_batch_lock = Lock() # Protects access to unprocessed_batch_messages

        # Placeholder for GPU FastAPI URL (assuming it's running locally for now)
        # In production, this would be the actual GPU instance's IP/DNS
        self.gpu_api_url = "http://localhost:8000/api/v1/process-batch"
        self.gpu_status_url = "http://localhost:8000/api/v1/status"


    def _rabbitmq_consumer_callback(self, message_data: Dict, channel: pika.adapters.blocking_connection.BlockingChannel, method: pika.spec.Basic.Deliver, properties: pika.spec.BasicProperties):
        """Callback executed when a message is received from RabbitMQ."""
        interview_id = message_data.get("interview_id")
        if not interview_id:
            print(f" [!] Received invalid message (no interview_id): {message_data}. Nacking.")
            channel.basic_nack(method.delivery_tag)
            return

        print(f" [x] Received interview_id: {interview_id} with delivery tag: {method.delivery_tag}")
        
        with self.unprocessed_batch_lock:
            # Check if this interview_id is already completed in MongoDB (idempotency)
            current_db_status = self.mongodb_handler.get_interview_status(interview_id)

            if current_db_status == ProcessingStatus.COMPLETED:
                print(f"Interview {interview_id} already COMPLETED in DB. Acknowledging and skipping.")
                channel.basic_ack(method.delivery_tag) # Immediately ack if already processed
                return

            # Check GCS for file completeness *before* adding to batch (simulated conversion complete)
            gcs_folder_prefix = f"{Config.GCS_AUDIO_ROOT_PREFIX}{interview_id}/"
            try:
                gcs_files = self.gcs_handler.list_files_in_prefix(gcs_folder_prefix)
                ref_natural_present = any(f.endswith(Config.REFERENCE_NATURAL_FILE) for f in gcs_files)
                ref_reading_present = any(f.endswith(Config.REFERENCE_READING_FILE) for f in gcs_files)
                segment_files_count = len([f for f in gcs_files if f.startswith(f"{Config.GCS_AUDIO_ROOT_PREFIX}{interview_id}/segment_")])

                if not (ref_natural_present and ref_reading_present and segment_files_count >= Config.MIN_EXPECTED_INTERVIEW_SEGMENTS):
                    print(f" [!] GCS folder for {interview_id} is incomplete or not found. Required refs: {ref_natural_present and ref_reading_present}, segments found: {segment_files_count}/{Config.MIN_EXPECTED_INTERVIEW_SEGMENTS}. Nacking for retry.")
                    channel.basic_nack(method.delivery_tag, requeue=True) # Nack to try again later
                    return
                
                # If GCS files are complete, add to internal batch list
                self.unprocessed_batch_messages.append({
                    "interview_id": interview_id,
                    "delivery_tag": method.delivery_tag,
                    "gcs_folder_prefix": gcs_folder_prefix # Store for later deletion
                })
                print(f"Added {interview_id} to internal batch list. Current count: {len(self.unprocessed_batch_messages)}")

                # Update MongoDB status (if not already processing)
                if current_db_status != ProcessingStatus.PROCESSING:
                    self.mongodb_handler.update_interview_status(
                        interview_id, ProcessingStatus.AUDIO_EXTRACTED_QUEUED
                    )

            except Exception as e:
                print(f" [!] Error checking GCS or DB for {interview_id}: {e}. Nacking for retry.")
                channel.basic_nack(method.delivery_tag, requeue=True)
                return

    def _wait_for_gpu_ready(self, timeout: int = Config.GPU_POLLING_TIMEOUT) -> bool:
        """Polls the GPU instance's health endpoint until it's ready."""
        start_time = time.time()
        print(f"Waiting for GPU instance at {self.gpu_status_url} to be ready...")
        while time.time() - start_time < timeout:
            try:
                response = requests.get(self.gpu_status_url, timeout=5) # Short timeout for poll
                response.raise_for_status() # Raises HTTPError for bad responses (4xx or 5xx)
                if response.json().get("status") == "running" and response.json().get("model_loaded"):
                    print("GPU instance is ready and model is loaded.")
                    return True
            except requests.exceptions.RequestException as e:
                print(f"GPU not ready yet: {e}. Retrying in {Config.GPU_POLLING_INTERVAL} seconds...")
            time.sleep(Config.GPU_POLLING_INTERVAL)
        print("Timeout waiting for GPU instance to become ready.")
        return False


    def _process_batch(self, batch_to_process: List[Dict[str, Any]]):
        """Sends a batch of interviews to the GPU instance for processing."""
        interview_ids_in_batch = [item["interview_id"] for item in batch_to_process]
        print(f"Attempting to send batch of {len(interview_ids_in_batch)} interviews to GPU: {interview_ids_in_batch}")

        try:
            # Here, you'd trigger the GPU VM if it's not already running
            # For this step, we'll assume it's manually started or always running locally
            # In a real scenario, this involves GCP Compute Engine API calls.
            # print("Simulating GPU instance spin-up (Not implemented in this script yet)...")
            
            # Wait for GPU to be truly ready (model loaded)
            if not self._wait_for_gpu_ready():
                print("GPU not ready. Batch will not be sent.")
                # You might nack messages here to put them back in queue
                # Or handle error state more robustly. For now, just print and return.
                return

            response = requests.post(self.gpu_api_url, json=interview_ids_in_batch, timeout=600) # Increased timeout for large batches
            response.raise_for_status()
            response_data = response.json()
            print(f"GPU API response for batch: {response_data}")

            # If the GPU successfully initiated processing, acknowledge the messages
            # Note: The GPU itself will update MongoDB status for individual interviews
            for item in batch_to_process:
                self.rabbitmq_client._channel.basic_ack(item["delivery_tag"])
                print(f"Acknowledged message for interview_id: {item['interview_id']}")

            # After batch is acknowledged, check MongoDB for COMPLETED status for each
            # This loop waits for all interviews in the batch to be truly done
            print(f"Waiting for individual interview statuses to become COMPLETED in DB...")
            all_completed = False
            start_wait_time = time.time()
            max_wait_time = 3600 # Wait up to 1 hour for batch to complete
            
            while not all_completed and (time.time() - start_wait_time < max_wait_time):
                all_completed = True
                for item in batch_to_process:
                    interview_id = item["interview_id"]
                    status = self.mongodb_handler.get_interview_status(interview_id)
                    if status != ProcessingStatus.COMPLETED:
                        all_completed = False
                        # print(f"  {interview_id} still {status.value if status else 'Not Found'}...")
                        break # Check next iteration

                if not all_completed:
                    time.sleep(10) # Poll MongoDB every 10 seconds

            if all_completed:
                print(f"All {len(interview_ids_in_batch)} interviews in batch processed successfully according to DB.")
                # --- GCS Cleanup ---
                for item in batch_to_process:
                    self.gcs_handler.delete_folder_by_prefix(item["gcs_folder_prefix"])
                print(f"Cleaned up GCS folders for processed batch.")
                
                # --- GPU Shutdown (Simulated) ---
                print("Simulating GPU instance shutdown (Not implemented in this script yet)...")
            else:
                print(f"Batch {interview_ids_in_batch} did not fully complete within {max_wait_time}s timeout.")
                # In a real system, you might trigger an alert here

        except requests.exceptions.RequestException as e:
            print(f"Error sending batch to GPU or receiving response: {e}")
            # If the GPU is unreachable or responds with error, nack the messages
            with self.unprocessed_batch_lock:
                for item in batch_to_process:
                    self.rabbitmq_client._channel.basic_nack(item["delivery_tag"], requeue=True) # Requeue messages
                    print(f"Nacked message for interview_id: {item['interview_id']} due to GPU error.")
            print("Batch messages re-queued due to GPU communication failure.")
        except Exception as e:
            print(f"Unhandled error during batch processing: {e}")
            # Decide on nack/ack for these unexpected errors
            with self.unprocessed_batch_lock:
                for item in batch_to_process:
                    self.rabbitmq_client._channel.basic_nack(item["delivery_tag"], requeue=True) 
            print("Batch messages re-queued due to unhandled error.")


    def run(self):
        """Starts the queue monitor."""
        print("Starting Queue Monitor...")
        # Start consuming RabbitMQ messages in a background thread
        self.rabbitmq_client.start_consuming(
            Config.RABBITMQ_PROCESSING_QUEUE,
            self._rabbitmq_consumer_callback
        )

        try:
            while True:
                # This loop checks if a batch is ready to be sent
                with self.unprocessed_batch_lock:
                    if len(self.unprocessed_batch_messages) >= self.gpu_trigger_threshold:
                        print(f"Threshold reached ({len(self.unprocessed_batch_messages)} >= {self.gpu_trigger_threshold}). Sending batch to GPU.")
                        # Take the current batch and clear the list
                        batch_to_send = self.unprocessed_batch_messages[:]
                        self.unprocessed_batch_messages.clear()
                        
                        # Process the batch in a new thread to not block the main loop
                        # This also allows for potential parallel batch processing if needed
                        batch_thread = Thread(target=self._process_batch, args=(batch_to_send,))
                        batch_thread.daemon = True # Allows main program to exit
                        batch_thread.start()
                    else:
                        print(f"Waiting for more interviews. Current count: {len(self.unprocessed_batch_messages)}/{self.gpu_trigger_threshold}")

                time.sleep(Config.GPU_POLLING_INTERVAL) # Check every few seconds
        except KeyboardInterrupt:
            print("\nQueue Monitor stopped.")
        except Exception as e:
            print(f"An error occurred in Queue Monitor main loop: {e}")
        finally:
            self.rabbitmq_client.stop_consuming()
            if self.mongodb_handler.client:
                self.mongodb_handler.client.close()
            print("Queue Monitor resources cleaned up.")

if __name__ == "__main__":
    monitor = QueueMonitor()
    monitor.run()