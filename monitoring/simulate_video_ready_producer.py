import pika
import json
import time
import uuid
import os
from config import Config

def send_message(interview_id: str, video_path: str):
    connection = None
    try:
        connection = pika.BlockingConnection(pika.ConnectionParameters(
            host=Config.RABBITMQ_HOST,
            port=Config.RABBITMQ_PORT,
            credentials=pika.PlainCredentials(Config.RABBITMQ_USER, Config.RABBITMQ_PASS)
        ))
        channel = connection.channel()

        queue_name = Config.RABBITMQ_VIDEO_READY_QUEUE
        channel.queue_declare(queue=queue_name, durable=True)

        # New payload format
        message = {"interview_id": interview_id, "path": video_path}
        channel.basic_publish(
            exchange='',
            routing_key=queue_name,
            body=json.dumps(message),
            properties=pika.BasicProperties(
                delivery_mode=2,  # make message persistent
            )
        )
        print(f" [x] Sent '{interview_id}' (video path: {video_path}) to queue '{queue_name}'")
    except pika.exceptions.AMQPConnectionError as e:
        print(f"ERROR: Could not connect to RabbitMQ at {Config.RABBITMQ_HOST}:{Config.RABBITMQ_PORT}. Is RabbitMQ server running? Error: {e}")
    except Exception as e:
        print(f"Error sending message: {e}")
    finally:
        if connection:
            connection.close()

if __name__ == "__main__":
    if not all([Config.RABBITMQ_HOST, Config.RABBITMQ_USER, Config.RABBITMQ_PASS]):
        print("ERROR: RabbitMQ credentials not fully loaded. Check your .env file and config.py.")
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
            {"id": "sim_video_001", "path": f"{raw_video_gcs_prefix}sim_video_001/stitched_video.mp4"},
        ]

        for video_info in test_video_data:
            send_message(video_info["id"], video_info["path"])
            time.sleep(1)
        print("\nSent all predefined test video messages. Exiting producer.")

    except KeyboardInterrupt:
        print("\nProducer stopped.")
    except Exception as e:
        print(f"An unexpected error occurred in producer: {e}")