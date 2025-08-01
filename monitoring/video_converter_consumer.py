from dotenv import load_dotenv

from config import Config
load_dotenv()

#import json
import time
from threading import Thread
from typing import Dict, Any
import pika

from app.services.rabbitmq_client import RabbitMQClient
from app.services.video_converter import VideoConverterService
from app.services.mongodb_handler import MongoDBHandler
from app.schemas.models import ProcessingStatus

class VideoConverterConsumer:
    def __init__(self):
        """
        self.rabbitmq_client = RabbitMQClient(
            host=Config.RABBITMQ_HOST,
            port=Config.RABBITMQ_PORT,
            user=Config.RABBITMQ_USER,
            password=Config.RABBITMQ_PASS
        )
        """
        print(f"RabbitMQClient: Attempting to connect using URL: {Config.RABBITMQ_URL}")
        self.rabbitmq_client = RabbitMQClient()
        self.video_converter_service = VideoConverterService()
        self.mongodb_handler = MongoDBHandler()

        self.video_ready_queue = Config.RABBITMQ_VIDEO_READY_QUEUE
        self.audio_processing_queue = Config.RABBITMQ_PROCESSING_QUEUE

    def _rabbitmq_consumer_callback(self, message_data: Dict[str, Any], channel: pika.adapters.blocking_connection.BlockingChannel, method: pika.spec.Basic.Deliver, properties: pika.spec.BasicProperties):
        video_id = message_data.get("interview_id") # Use interview_id for consistency
        gcs_video_path = message_data.get("path") # Use 'path' as per new payload

        if not video_id or not gcs_video_path:
            print(f" [!] Received invalid video message (missing interview_id or path): {message_data}. Nacking.")
            channel.basic_nack(method.delivery_tag, requeue=False)
            return

        print(f" [x] Received video_id: {video_id} from {gcs_video_path}")
        
        # Update MongoDB status to indicate video conversion is starting
        self.mongodb_handler.add_history_entry(
            video_id,
            status=ProcessingStatus.PROCESSING,
            stage="video_conversion",
            actor="video_converter_consumer",
            message="Started video to audio conversion.",
            video_gcs_path=gcs_video_path
        )

        try:
            # Convert Windows-style path to GCS-compatible path (forward slashes)
            gcs_video_path_clean = gcs_video_path.replace("\\", "/")

            # Perform video to audio conversion and upload segments to GCS
            gcs_audio_prefix = self.video_converter_service.convert_video_to_audio_segments(video_id, gcs_video_path_clean)

            if gcs_audio_prefix:
                # Publish message to the next queue (audio processing queue)
                audio_ready_message = {"interview_id": video_id, "gcs_audio_prefix": gcs_audio_prefix}
                self.rabbitmq_client.publish_message(self.audio_processing_queue, audio_ready_message, close_after_publish=False)
                print(f" [x] Published audio ready message for {video_id} to {self.audio_processing_queue}")
                
                # Update MongoDB status to AUDIO_EXTRACTED_QUEUED with history
                self.mongodb_handler.add_history_entry(
                    video_id,
                    status=ProcessingStatus.AUDIO_EXTRACTED_QUEUED,
                    stage="video_conversion",
                    actor="video_converter_consumer",
                    message="Video conversion completed and audio segments uploaded.",
                    audio_gcs_prefix=gcs_audio_prefix
                )
                
                channel.basic_ack(method.delivery_tag)
                print(f" [x] Acknowledged video message for {video_id}.")
            else:
                error_msg = f"Video conversion for {video_id} resulted in no audio segments or insufficient segments."
                print(f" [!] {error_msg}. Nacking video message.")
                self.mongodb_handler.add_history_entry(
                    video_id,
                    status=ProcessingStatus.FAILED,
                    stage="video_conversion",
                    actor="video_converter_consumer",
                    error=error_msg,
                    message="Video conversion produced no/insufficient segments.",
                    requeue=False # Do not requeue if no segments were produced
                )
                channel.basic_nack(method.delivery_tag, requeue=False)

        except Exception as e:
            error_msg = f"Error converting video {video_id}: {str(e)}"
            print(f" [!] {error_msg}. Nacking video message for retry.")
            self.mongodb_handler.add_history_entry(
                video_id,
                status=ProcessingStatus.FAILED,
                stage="video_conversion",
                actor="video_converter_consumer",
                error=error_msg,
                message="Video conversion failed.",
                requeue=True # Requeue for retry
            )
            channel.basic_nack(method.delivery_tag, requeue=True)

    def run(self):
        print(f"Starting Video Converter Consumer. Listening to queue: {self.video_ready_queue}")
        self.rabbitmq_client.start_consuming(
            self.video_ready_queue,
            self._rabbitmq_consumer_callback
        )
        try:
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
    consumer = VideoConverterConsumer()
    consumer.run()