from dotenv import load_dotenv
load_dotenv() # Load env variables for config

import json
import time
from typing import Dict, Any, Callable
import pika

from app.services.audio_processor import AudioProcessorService
from app.services.rabbitmq_client import RabbitMQClient
from app.services.mongodb_handler import MongoDBHandler # Ensure this is imported for clean shutdown
from config import Config
from app.schemas.models import ProcessingStatus

def process_single_message(message_data: Dict[str, Any], channel: pika.adapters.blocking_connection.BlockingChannel, method: pika.spec.Basic.Deliver, properties: pika.spec.BasicProperties, audio_service: AudioProcessorService):
    """
    Handles the processing of a single message from the RabbitMQ queue.
    This function will be called by the RabbitMQClient's drain_queue_and_process.
    """
    interview_id = message_data.get("interview_id")
    gcs_audio_prefix = message_data.get("gcs_audio_prefix")

    if not interview_id or not gcs_audio_prefix:
        print(f" [!] Received invalid message (missing interview_id or gcs_audio_prefix): {message_data}. Nacking.")
        channel.basic_nack(method.delivery_tag, requeue=False) # Don't requeue malformed messages
        return

    print(f"Processing interview_id: {interview_id} from GCS prefix: {gcs_audio_prefix}")
    
    try:
        # Pass the interview_id and let audio_service handle status updates, downloads, processing, and MongoDB storage
        # The audio_service's method now manages per-interview status updates and history
        audio_service.process_single_interview_from_gcs(interview_id)
        
        # Acknowledge the message only after successful processing and storage
        channel.basic_ack(method.delivery_tag)
        print(f" [x] Successfully processed and acknowledged message for interview_id: {interview_id}")

    except Exception as e:
        error_message = f"Error processing interview {interview_id}: {str(e)}"
        print(f" [!] {error_message}. Nacking message for retry.")
        # The audio_service.process_single_interview_from_gcs already added a FAILED history entry
        # We just need to nack the RabbitMQ message here.
        channel.basic_nack(method.delivery_tag, requeue=True) # Requeue for retry

def run_ml_batch_processing():
    print("ML Batch Processor (Cloud Run Job) starting...")
    audio_service = None
    rabbitmq_client = None
    mongodb_handler = None # For explicit shutdown

    try:
        audio_service = AudioProcessorService() # Model loaded here
        rabbitmq_client = RabbitMQClient() # Connects to RabbitMQ
        mongodb_handler = MongoDBHandler() # Connects to MongoDB (already done by audio_service, but ensures close)

        # Create a partial function to pass audio_service to the message processor
        # This avoids re-initializing audio_service (and model) for each message
        process_func_with_service = lambda msg, ch, method, props: process_single_message(msg, ch, method, props, audio_service)

        # Drain all available messages from the ML processing queue
        rabbitmq_client.drain_queue_and_process(Config.RABBITMQ_PROCESSING_QUEUE, process_func_with_service)
        
    except Exception as e:
        print(f"An unexpected critical error occurred in the ML Batch Processor: {e}")
        # Log this error to stdout/stderr, which Cloud Run will capture.
    finally:
        # Ensure all resources are cleanly shut down
        if rabbitmq_client:
            rabbitmq_client._close_connection() # Use internal close for one-shot job
            print("RabbitMQ connection closed by ML Batch Processor.")
        if audio_service:
            audio_service.cleanup() # This closes MongoDB client as well
        print("ML Batch Processor finished. Resources cleaned up.")

if __name__ == "__main__":
    run_ml_batch_processing()