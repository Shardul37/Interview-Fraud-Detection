import pika
import json
import time
import os
from threading import Thread
from typing import Dict, Any
from config import Config
from app.services.rabbitmq_client import RabbitMQClient
from app.services.video_converter import VideoConverterService # NEW IMPORT
from app.services.mongodb_handler import MongoDBHandler # For status updates
from app.schemas.models import ProcessingStatus # For status updates

class VideoConverterConsumer:
    def __init__(self):
        self.rabbitmq_client = RabbitMQClient(
            host=Config.RABBITMQ_HOST,
            port=Config.RABBITMQ_PORT,
            user=Config.RABBITMQ_USER,
            password=Config.RABBITMQ_PASS
        )
        self.video_converter_service = VideoConverterService() # Initialize video converter
        self.mongodb_handler = MongoDBHandler() # For DB updates

        self.video_ready_queue = Config.RABBITMQ_VIDEO_READY_QUEUE
        self.audio_processing_queue = Config.RABBITMQ_PROCESSING_QUEUE

    def _rabbitmq_consumer_callback(self, message_data: Dict[str, Any], channel: pika.adapters.blocking_connection.BlockingChannel, method: pika.spec.Basic.Deliver, properties: pika.spec.BasicProperties):
        """Callback executed when a raw video message is received."""
        video_id = message_data.get("video_id")
        gcs_video_path = message_data.get("gcs_video_path")

        if not video_id or not gcs_video_path:
            print(f" [!] Received invalid video message: {message_data}. Nacking.")
            channel.basic_nack(method.delivery_tag, requeue=False)
            return

        print(f" [x] Received video_id: {video_id} from {gcs_video_path}")
        
        # Update MongoDB status to indicate video conversion is starting
        self.mongodb_handler.update_interview_status(
            video_id, ProcessingStatus.PROCESSING, # Use PROCESSING for video conversion too
            video_gcs_path=gcs_video_path,
            processing_stage="video_conversion"
        )

        try:
            # Perform video to audio conversion and upload segments to GCS
            gcs_audio_prefix = self.video_converter_service.convert_video_to_audio_segments(video_id, gcs_video_path)

            if gcs_audio_prefix:
                # Publish message to the next queue (audio processing queue)
                audio_ready_message = {"interview_id": video_id, "gcs_audio_prefix": gcs_audio_prefix}
                self.rabbitmq_client.publish_message(self.audio_processing_queue, audio_ready_message, close_after_publish=False)
                print(f" [x] Published audio ready message for {video_id} to {self.audio_processing_queue}")
                
                # Update MongoDB status to AUDIO_EXTRACTED_QUEUED
                self.mongodb_handler.update_interview_status(
                    video_id, ProcessingStatus.AUDIO_EXTRACTED_QUEUED,
                    audio_gcs_prefix=gcs_audio_prefix,
                    processing_stage="audio_extracted"
                )
                
                channel.basic_ack(method.delivery_tag) # Acknowledge the video message
                print(f" [x] Acknowledged video message for {video_id}.")
            else:
                error_msg = f"Video conversion for {video_id} resulted in no audio segments."
                print(f" [!] {error_msg}. Nacking video message.")
                self.mongodb_handler.update_interview_status(
                    video_id, ProcessingStatus.FAILED, error_message=error_msg, processing_stage="video_conversion_failed"
                )
                channel.basic_nack(method.delivery_tag, requeue=False) # Do not requeue if no segments were produced

        except Exception as e:
            error_msg = f"Error converting video {video_id}: {str(e)}"
            print(f" [!] {error_msg}. Nacking video message for retry.")
            self.mongodb_handler.update_interview_status(
                video_id, ProcessingStatus.FAILED, error_message=error_msg, processing_stage="video_conversion_failed"
            )
            channel.basic_nack(method.delivery_tag, requeue=True) # Requeue for retry

    def run(self):
        """Starts the video converter consumer."""
        print(f"Starting Video Converter Consumer. Listening to queue: {self.video_ready_queue}")
        # Start consuming RabbitMQ messages in a background thread
        self.rabbitmq_client.start_consuming(
            self.video_ready_queue,
            self._rabbitmq_consumer_callback
        )
        try:
            # Keep the main thread alive
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            print("\nVideo Converter Consumer stopped.")
        finally:
            self.rabbitmq_client.stop_consuming()
            if self.mongodb_handler.client:
                self.mongodb_handler.close()
            print("Video Converter Consumer resources cleaned up.")

if __name__ == "__main__":
    # Ensure .env variables are loaded for this script
    from dotenv import load_dotenv
    load_dotenv()
    
    # Simple check for essential configs
    if not all([Config.RABBITMQ_HOST, Config.MONGO_URI, Config.GCS_BUCKET_NAME]):
        print("ERROR: Essential configuration variables are missing. Please check your .env file and config.py.")
        exit(1)

    consumer = VideoConverterConsumer()
    consumer.run()