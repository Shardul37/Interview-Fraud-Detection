from dotenv import load_dotenv
load_dotenv() # Load env variables for config

from typing import Dict, Any
import pika
import asyncio

from app.services.audio_processor import AudioProcessorService
from app.services.rabbitmq_client import RabbitMQClient
from config import Config

async def process_single_message_async(message_data: Dict[str, Any], channel: pika.adapters.blocking_connection.BlockingChannel, method: pika.spec.Basic.Deliver, properties: pika.spec.BasicProperties, audio_service: AudioProcessorService):
    """
    Handles the processing of a single message from the RabbitMQ queue.
    This function will be called by the RabbitMQClient's drain_queue_and_process
    and needs to be awaited.
    """
    interview_id = message_data.get("interview_id")
    gcs_audio_prefix = message_data.get("gcs_audio_prefix")

    if not interview_id or not gcs_audio_prefix:
        print(f" [!] Received invalid message (missing interview_id or gcs_audio_prefix): {message_data}. Nacking.")
        channel.basic_nack(method.delivery_tag, requeue=False) # Don't requeue malformed messages
        return

    print(f"Processing interview_id: {interview_id} from GCS prefix: {gcs_audio_prefix}")
    
    try:
        # AWAIT the coroutine function call
        await audio_service.process_single_interview_from_gcs(interview_id)
        
        # Acknowledge the message only after successful processing and storage
        channel.basic_ack(method.delivery_tag)
        print(f" [x] Successfully processed and acknowledged message for interview_id: {interview_id}")

    except Exception as e:
        error_message = f"Error processing interview {interview_id}: {str(e)}"
        print(f" [!] {error_message}. Nacking message for retry.")
        # The audio_service.process_single_interview_from_gcs already added a FAILED history entry
        # We just need to nack the RabbitMQ message here.
        channel.basic_nack(method.delivery_tag, requeue=True) # Requeue for retry


# Modified run_ml_batch_processing to use asyncio.run for each message
def run_ml_batch_processing():
    print("ML Batch Processor (Cloud Run Job) starting...")
    audio_service = None
    rabbitmq_client = None

    try:
        # Initialize services
        audio_service = AudioProcessorService()
        rabbitmq_client = RabbitMQClient()

        # Define the processing function (which wraps the async part)
        # This wrapper allows the synchronous drain_queue_and_process to call an async function
        def sync_wrapper_process_message(message_data, channel, method, properties):
            # Run the async processing function for each message using asyncio.run
            asyncio.run(process_single_message_async(message_data, channel, method, properties, audio_service))

        # Drain all available messages from the ML processing queue
        rabbitmq_client.drain_queue_and_process(Config.RABBITMQ_PROCESSING_QUEUE, sync_wrapper_process_message)

    except Exception as e:
        print(f"An unexpected critical error occurred in the ML Batch Processor: {e}")
    finally:
        # Ensure all resources are cleanly shut down
        if rabbitmq_client:
            # Use the internal close for the rabbitmq_client as it's a one-shot job
            rabbitmq_client._close_connection() 
            print("RabbitMQ connection closed by ML Batch Processor.")
        if audio_service:
            # audio_service.cleanup() will also close the MongoDB client
            audio_service.cleanup() 
        print("ML Batch Processor finished. Resources cleaned up.")

if __name__ == "__main__":
    run_ml_batch_processing()