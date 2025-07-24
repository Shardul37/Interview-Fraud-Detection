# monitoring/simulate_audio_ready_producer.py
import pika
import json
import time
import uuid
import os
from config import Config # Ensure config.py is in a path accessible to this script

def send_message(interview_id: str):
    connection = None
    try:
        connection = pika.BlockingConnection(pika.ConnectionParameters(
            host=Config.RABBITMQ_HOST,
            port=Config.RABBITMQ_PORT,
            credentials=pika.PlainCredentials(Config.RABBITMQ_USER, Config.RABBITMQ_PASS)
        ))
        channel = connection.channel()

        queue_name = Config.RABBITMQ_PROCESSING_QUEUE
        channel.queue_declare(queue=queue_name, durable=True)

        message = {"interview_id": interview_id}
        channel.basic_publish(
            exchange='',
            routing_key=queue_name,
            body=json.dumps(message),
            properties=pika.BasicProperties(
                delivery_mode=2,  # make message persistent
            )
        )
        print(f" [x] Sent '{interview_id}' to queue '{queue_name}'")
    except pika.exceptions.AMQPConnectionError as e:
        print(f"ERROR: Could not connect to RabbitMQ: {e}. Is RabbitMQ server running?")
    except Exception as e:
        print(f"Error sending message: {e}")
    finally:
        if connection:
            connection.close()

if __name__ == "__main__":
    print(f"Simulating audio ready messages. Sending to RabbitMQ queue: {Config.RABBITMQ_PROCESSING_QUEUE}")
    print("Ensure RabbitMQ is running and your .env is configured correctly.")
    print("Remember to manually upload corresponding GCS folders for each predefined interview ID!")
    
    # --- TEMPORARY TEST MODE ---
    # Define a list of interview IDs that you have ALREADY manually set up in GCS
    test_interview_ids = [
        "simulated_interview_0cd3a115",
        "simulated_interview_5a3a67db",
        #"predefined_interview_003", # Add more if your threshold is higher
    ]
    
    # Send each message once and exit
    for test_id in test_interview_ids:
        send_message(test_id)
        time.sleep(1) # Small delay
    print("\nSent all predefined test messages. Exiting producer.")
    # --- END TEMPORARY TEST MODE ---
    
    #print("Remember to manually upload corresponding GCS folders (e.g., 'test_extracted_audio/simulated_interview_XXXX/')!")
    #print("Press Ctrl+C to exit.")
    #try:
    #    while True:
    #        new_interview_id = f"simulated_interview_{uuid.uuid4().hex[:8]}"
    #        send_message(new_interview_id)
    #        time.sleep(5) # Send a new message every 5 seconds
    #except KeyboardInterrupt:
    #    print("\nProducer stopped.")
 