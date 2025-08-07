import time
from config import Config
from app.services.rabbitmq_client import RabbitMQClient

def send_message_to_ml_queue(interview_id: str, gcs_audio_prefix: str):
    rabbitmq_client = None
    try:
        rabbitmq_client = RabbitMQClient()
        
        # Ensure the queue for ML processing exists
        queue_name = Config.RABBITMQ_PROCESSING_QUEUE
        rabbitmq_client.ensure_queue_exists(queue_name, durable=True)
        
        # Payload for the ML processing service
        message = {"interview_id": interview_id, "gcs_audio_prefix": gcs_audio_prefix}
        rabbitmq_client.publish_message(queue_name, message, close_after_publish=True)
        print(f" [x] Sent '{message}' to queue '{queue_name}'")
    except Exception as e:
        print(f"Error sending message to ML queue: {e}")
    finally:
        if rabbitmq_client:
            rabbitmq_client._close_connection()

if __name__ == "__main__":
    if not Config.RABBITMQ_URL:
        print("ERROR: RabbitMQ URL not loaded. Check your .env file and config.py.")
        exit(1)

    print(f"Simulating audio ready messages. Sending to RabbitMQ queue: {Config.RABBITMQ_PROCESSING_QUEUE}")
    print("IMPORTANT: Ensure audio segment files exist in GCS for the specified paths!")
    print("Press Ctrl+C to exit.")
    try:
        # Example GCS audio prefix for the extracted segments
        gcs_audio_prefix = f"{Config.GCS_AUDIO_ROOT_PREFIX}stitched_video-VEED/"
        
        # Test data matching the payload format for the ML service
        test_audio_data = [
            {"id": "sim_video_001", "path": gcs_audio_prefix},
            {"id": "sim_video_002", "path": gcs_audio_prefix},
        ]

        for audio_info in test_audio_data:
            send_message_to_ml_queue(audio_info["id"], audio_info["path"])
            time.sleep(1)
        print("\nSent all predefined test audio messages. Exiting producer.")

    except KeyboardInterrupt:
        print("\nProducer stopped.")
    except Exception as e:
        print(f"An unexpected error occurred in producer: {e}")