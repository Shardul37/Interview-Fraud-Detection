import time
from config import Config
from app.services.rabbitmq_client import RabbitMQClient

def send_message(interview_id: str, video_path: str):
    rabbitmq_client = None
    try:
        rabbitmq_client = RabbitMQClient()
        
        # Ensure the queue exists
        queue_name = Config.RABBITMQ_VIDEO_READY_QUEUE
        rabbitmq_client.ensure_queue_exists(queue_name, durable=True)
        
        # New payload format
        message = {"interview_id": interview_id, "path": video_path}
        rabbitmq_client.publish_message(queue_name, message, close_after_publish=True)
    except Exception as e:
        print(f"Error sending message: {e}")
    finally:
        if rabbitmq_client:
            rabbitmq_client._close_connection()

if __name__ == "__main__":
    if not Config.RABBITMQ_URL:
        print("ERROR: RabbitMQ URL not loaded. Check your .env file and config.py.")
        exit(1)

    print(f"Simulating video ready messages. Sending to RabbitMQ queue: {Config.RABBITMQ_VIDEO_READY_QUEUE}")
    print("Ensure RabbitMQ server is running and accessible.")
    print("IMPORTANT: Manually upload corresponding MP4 video files to GCS (e.g., 'shardul_test/test_videos/sim_video_00X.mp4')!")
    print("Press Ctrl+C to exit.")
    try:
        # Example GCS path for raw videos. Adjust this to your actual GCS prefix.
        raw_video_gcs_prefix = "shardul_test/test_videos/"
        
        # Test video data matching the payload format you provided
        test_video_data = [
            {"id": "stitched_video-VEED", "path": f"{raw_video_gcs_prefix}stitched_video-VEED/stitched_video-VEED.mp4"},
        ]

        for video_info in test_video_data:
            send_message(video_info["id"], video_info["path"])
            time.sleep(1)
        print("\nSent all predefined test video messages. Exiting producer.")

    except KeyboardInterrupt:
        print("\nProducer stopped.")
    except Exception as e:
        print(f"An unexpected error occurred in producer: {e}")