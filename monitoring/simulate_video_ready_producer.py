import pika
import json
import time
import uuid
import os
from config import Config

def send_message(video_id: str, gcs_video_path: str):
    connection = None
    try:
        connection = pika.BlockingConnection(pika.ConnectionParameters(
            host=Config.RABBITMQ_HOST,
            port=Config.RABBITMQ_PORT,
            credentials=pika.PlainCredentials(Config.RABBITMQ_USER, Config.RABBITMQ_PASS)
        ))
        channel = connection.channel()

        queue_name = Config.RABBITMQ_VIDEO_READY_QUEUE # Send to the new video queue
        channel.queue_declare(queue=queue_name, durable=True)

        message = {"video_id": video_id, "gcs_video_path": gcs_video_path}
        channel.basic_publish(
            exchange='',
            routing_key=queue_name,
            body=json.dumps(message),
            properties=pika.BasicProperties(
                delivery_mode=2,  # make message persistent
            )
        )
        print(f" [x] Sent '{video_id}' (video path: {gcs_video_path}) to queue '{queue_name}'")
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
    print("IMPORTANT: Manually upload corresponding MP4 video files to GCS (e.g., 'raw_videos/simulated_video_XXXX.mp4')!")
    print("Press Ctrl+C to exit.")
    try:
        # Define a list of video IDs and their GCS paths you've manually uploaded for testing
        # Replace 'your_raw_videos_bucket_prefix' with the actual GCS prefix where raw videos are stored
        # e.g., "raw_videos/"
        raw_video_gcs_prefix = "shardul_test/test_videos/" # Adjust this to your actual raw video GCS prefix (NOTE: bucket name should NOT be included)
        
        test_video_data = [
            #{"id": "sim_video_001", "path": f"{raw_video_gcs_prefix}sim_video_001.mp4"},
            {"id": "sim_video_001", "path": f"{raw_video_gcs_prefix}sim_video_002.mp4"},            # Add more as needed for testing your batching threshold
        ]

        # Send each message once and exit
        for video_info in test_video_data:
            send_message(video_info["id"], video_info["path"])
            time.sleep(1) # Small delay
        print("\nSent all predefined test video messages. Exiting producer.")

        # Uncomment the while loop below if you want random, continuous video messages again
        # while True:
        #     new_video_id = f"simulated_video_{uuid.uuid4().hex[:8]}"
        #     # You'd need to manually upload a video for each random ID or use a fixed one
        #     fixed_gcs_video_path = f"{raw_video_gcs_prefix}your_test_video.mp4" # Example fixed path (NOTE: Do NOT include bucket name in path)
        #     send_message(new_video_id, fixed_gcs_video_path)
        #     time.sleep(5)
    except KeyboardInterrupt:
        print("\nProducer stopped.")
    except Exception as e:
        print(f"An unexpected error occurred in producer: {e}")