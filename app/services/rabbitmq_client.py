# app/services/rabbitmq_client.py
import pika
import json
import time
from threading import Thread
from typing import Callable, Optional, Dict
from config import Config

class RabbitMQClient:
    def __init__(self, host=Config.RABBITMQ_HOST, port=Config.RABBITMQ_PORT, user=Config.RABBITMQ_USER, password=Config.RABBITMQ_PASS):
        self.credentials = pika.PlainCredentials(user, password)
        self.connection_params = pika.ConnectionParameters(
            host=host,
            port=port,
            credentials=self.credentials,
            heartbeat=600 # Helps maintain connection stability
        )
        self._connection = None
        self._channel = None
        self._consumer_thread: Optional[Thread] = None
        self._is_consuming = False

    def _connect(self):
        """Establishes a connection and channel."""
        try:
            if not self._connection or self._connection.is_closed:
                self._connection = pika.BlockingConnection(self.connection_params)
                self._channel = self._connection.channel()
                print("RabbitMQ connection established.")
        except pika.exceptions.AMQPConnectionError as e:
            print(f"Failed to connect to RabbitMQ: {e}")
            self._connection = None
            self._channel = None
            raise

    def ensure_queue_exists(self, queue_name: str, durable: bool = True, auto_delete: bool = False, 
                           exclusive: bool = False, arguments: Optional[Dict] = None):
        """
        Explicitly ensures a queue exists with specific configuration.
        This method makes queue creation intentional and configurable.
        
        Args:
            queue_name: Name of the queue
            durable: Queue survives server restarts
            auto_delete: Queue deleted when no consumers
            exclusive: Queue only accessible by this connection
            arguments: Additional queue arguments (TTL, max length, etc.)
        """
        try:
            self._connect()
            self._channel.queue_declare(
                queue=queue_name, 
                durable=durable,
                auto_delete=auto_delete,
                exclusive=exclusive,
                arguments=arguments or {}
            )
            print(f"Queue '{queue_name}' ensured with configuration: durable={durable}")
            return True
        except Exception as e:
            print(f"Failed to ensure queue '{queue_name}' exists: {e}")
            raise

    def publish_message(self, queue_name: str, message: Dict, close_after_publish: bool = True):
        """Publishes a message to the specified queue."""
        try:
            self._connect()
            # Ensure queue exists before publishing
            self.ensure_queue_exists(queue_name, durable=True)
            
            self._channel.basic_publish(
                exchange='',
                routing_key=queue_name,
                body=json.dumps(message),
                properties=pika.BasicProperties(
                    delivery_mode=2,  # make message persistent
                )
            )
            print(f" [x] Sent '{message}' to queue '{queue_name}'")
        except Exception as e:
            print(f"Error publishing message to {queue_name}: {e}")
            # Re-raise to let caller handle, or implement retry logic
            raise
        finally:
            if close_after_publish:
                self._close_connection() # Close connection after publish for simplicity in short-lived scripts

    def start_consuming(self, queue_name: str, callback: Callable[[Dict, pika.adapters.blocking_connection.BlockingChannel, pika.spec.Basic.Deliver, pika.spec.BasicProperties], None]):
        """Starts consuming messages from a queue in a separate thread."""
        self._queue_name = queue_name
        self._callback = callback
        self._is_consuming = True
        self._consumer_thread = Thread(target=self._consumer_loop)
        self._consumer_thread.daemon = True # Allow main program to exit even if thread is running
        self._consumer_thread.start()
        print(f"Started consuming from queue '{queue_name}' in background thread.")

    def _consumer_loop(self):
        """The actual consuming loop for the thread."""
        while self._is_consuming:
            try:
                self._connect()
                # Ensure queue exists before consuming
                self.ensure_queue_exists(self._queue_name, durable=True)
                
                # Use a lambda to wrap the actual callback, so we can pass channel, method, properties
                def wrapper_callback(ch, method, properties, body):
                    try:
                        message_data = json.loads(body.decode('utf-8'))
                        self._callback(message_data, ch, method, properties)
                    except json.JSONDecodeError as e:
                        print(f"Error decoding message body: {e} - Body: {body}")
                        ch.basic_nack(method.delivery_tag) # Nack invalid message
                    except Exception as e:
                        print(f"Error in consumer callback: {e}")
                        # Depending on severity, you might nack here or let it retry after crash
                        ch.basic_nack(method.delivery_tag) # Nack to retry message later
                        
                self._channel.basic_consume(
                    queue=self._queue_name,
                    on_message_callback=wrapper_callback,
                    auto_ack=False # Important: Manual acknowledgement for reliable processing
                )
                print(f"Consumer listening for messages on '{self._queue_name}'...")
                self._channel.start_consuming()
            except pika.exceptions.AMQPConnectionError as e:
                print(f"RabbitMQ connection lost, attempting to reconnect in 5 seconds... ({e})")
                self._connection = None
                self._channel = None
                time.sleep(5)
            except Exception as e:
                print(f"Unhandled error in consumer loop: {e}. Reattempting in 5 seconds.")
                time.sleep(5)

    def stop_consuming(self):
        """Stops the consumer thread."""
        self._is_consuming = False
        if self._channel:
            try:
                self._channel.stop_consuming()
                print("RabbitMQ consumer stopped.")
            except Exception as e:
                print(f"Error stopping RabbitMQ consumer: {e}")
        self._close_connection()
        if self._consumer_thread and self._consumer_thread.is_alive():
            self._consumer_thread.join(timeout=10) # Wait for thread to finish
            if self._consumer_thread.is_alive():
                print("Warning: Consumer thread did not terminate gracefully.")


    def _close_connection(self):
        """Closes the RabbitMQ connection if it's open."""
        if self._connection and self._connection.is_open:
            self._connection.close()
            # print("RabbitMQ connection closed.")