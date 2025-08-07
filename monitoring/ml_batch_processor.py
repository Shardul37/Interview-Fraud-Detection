import sys
import signal
import time
import json
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
    Handles the processing of a single message from the RabbitMQ queue
    This function is called by the synchronous `drain_queue_and_process` method
    within a synchronous wrapper. It processes a single message from the queue
    and must be awaited to handle its asynchronous operations (like the
    `audio_service.process_single_interview_from_gcs` call).
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


# Global variables for graceful shutdown
audio_service = None
rabbitmq_client = None
is_running = True

def signal_handler(signum, frame):
    """Handle graceful shutdown on SIGINT or SIGTERM"""
    global is_running
    print(f"\nReceived signal {signum}. Initiating graceful shutdown...")
    is_running = False

def run_ml_batch_processing():
    print("ML Batch Processor (Continuous Service) starting...")
    global audio_service, rabbitmq_client, is_running
    
    # Set up signal handlers for graceful shutdown
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    try:
        # Initialize services
        audio_service = AudioProcessorService()
        rabbitmq_client = RabbitMQClient()

        print(f"Starting continuous consumption of queue: {Config.RABBITMQ_PROCESSING_QUEUE}")
        
        # Define the processing function (which wraps the async part) - same as original
        def sync_wrapper_process_message(message_data, channel, method, properties):
            # Run the async processing function for each message using asyncio.run
            asyncio.run(process_single_message_async(message_data, channel, method, properties, audio_service))
        
        # Continuous version of drain_queue_and_process
        while is_running:
            try:
                # Connect and set up (only once per connection)
                rabbitmq_client._connect()
                rabbitmq_client.ensure_queue_exists(Config.RABBITMQ_PROCESSING_QUEUE, durable=True)
                rabbitmq_client._channel.basic_qos(prefetch_count=1)
                
                print(f"Checking for messages in queue '{Config.RABBITMQ_PROCESSING_QUEUE}'...")
                
                # Process all available messages (like original drain logic)
                messages_processed = 0
                while is_running:
                    try:
                        method_frame, properties, body = rabbitmq_client._channel.basic_get(
                            queue=Config.RABBITMQ_PROCESSING_QUEUE, auto_ack=False
                        )
                        
                        if method_frame is None:
                            # Queue is empty, exit inner loop to wait and reconnect
                            if messages_processed > 0:
                                print(f"Processed {messages_processed} messages. Queue is now empty.")
                            break
                        
                        # Process the message using the same logic as original
                        try:
                            message_data = json.loads(body.decode('utf-8'))
                            print(f" [x] Got message from queue: {message_data.get('interview_id')}")
                            sync_wrapper_process_message(message_data, rabbitmq_client._channel, method_frame, properties)
                            messages_processed += 1
                        except json.JSONDecodeError as e:
                            print(f" [!] Error decoding message body: {e} - Body: {body}. Nacking.")
                            rabbitmq_client._channel.basic_nack(method_frame.delivery_tag, requeue=False)
                        except Exception as e:
                            print(f" [!] Error processing message: {e}. Nacking for retry.")
                            rabbitmq_client._channel.basic_nack(method_frame.delivery_tag, requeue=True)
                    
                    except pika.exceptions.AMQPConnectionError as e:
                        print(f"RabbitMQ connection lost during processing: {e}")
                        break  # Exit inner loop to reconnect
                
                # Close connection after processing batch (like original)
                rabbitmq_client._close_connection()
                
                # Wait before checking for more messages (only when queue was empty)
                if is_running and messages_processed == 0:
                    print("Queue empty. Waiting 3 seconds before checking again...")
                    time.sleep(3)
                    
            except KeyboardInterrupt:
                print("\nKeyboard interrupt received. Stopping...")
                break
            except pika.exceptions.AMQPConnectionError as e:
                print(f"RabbitMQ connection error, attempting to reconnect in 5 seconds... ({e})")
                time.sleep(5)
            except Exception as e:
                print(f"Unhandled error in processing loop: {e}. Retrying in 5 seconds.")
                time.sleep(5)

    except Exception as e:
        print(f"An unexpected critical error occurred in the ML Batch Processor: {e}")
    finally:
        # Ensure all resources are cleanly shut down
        print("Shutting down ML Batch Processor...")
        if rabbitmq_client:
            rabbitmq_client._close_connection()
            print("RabbitMQ connection closed by ML Batch Processor.")
        if audio_service:
            # audio_service.cleanup() will also close the MongoDB client
            audio_service.cleanup() 
        print("ML Batch Processor shutdown complete. Resources cleaned up.")

if __name__ == "__main__":
    run_ml_batch_processing()