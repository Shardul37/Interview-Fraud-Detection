import pika
import json
import time
from threading import Thread
from typing import Callable, Optional, Dict, Any
from config import Config

class RabbitMQClient:
    """
    def __init__(self, host=Config.RABBITMQ_HOST, port=Config.RABBITMQ_PORT, user=Config.RABBITMQ_USER, password=Config.RABBITMQ_PASS):
        self.credentials = pika.PlainCredentials(user, password)
        self.connection_params = pika.ConnectionParameters(
            host=host,
            port=port,
            credentials=self.credentials,
            heartbeat=600
        )
        print(f"DEBUG: Attempting to connect to RabbitMQ at {host}:{port} with user {user}")
        
        self._connection = None
        self._channel = None
        self._consumer_thread: Optional[Thread] = None
        self._is_consuming = False
        self._connect() # Connect on init
    """
    def __init__(self):
        # We now rely entirely on Config.RABBITMQ_URL which will contain
        # the full AMQP URL string (e.g., 'amqp://user:pass@host:port/').
        # pika.URLParameters will parse this string into connection parameters.
        self.connection_params = pika.URLParameters(Config.RABBITMQ_URL)
        self._connection = None
        self._channel = None
        self._consumer_thread: Optional[Thread] = None
        self._is_consuming = False
        self._connect() # Connect on init

    def _connect(self):
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

    def ensure_queue_exists(self, queue_name: str, durable: bool = True, auto_delete: bool = False, exclusive: bool = False, arguments: Optional[Dict] = None):
        try:
            self._connect() # Ensure connection before declaring
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

    def publish_message(self, queue_name: str, message: Dict, close_after_publish: bool = False):
        try:
            self._connect()
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
            raise
        finally:
            if close_after_publish:
                self._close_connection()

    def start_consuming(self, queue_name: str, callback: Callable[[Dict, pika.adapters.blocking_connection.BlockingChannel, pika.spec.Basic.Deliver, pika.spec.BasicProperties], None]):
        self._queue_name = queue_name
        self._callback = callback
        self._is_consuming = True
        self._consumer_thread = Thread(target=self._consumer_loop)
        self._consumer_thread.daemon = True
        self._consumer_thread.start()
        print(f"Started consuming from queue '{queue_name}' in background thread.")

    def _consumer_loop(self):
        while self._is_consuming:
            try:
                self._connect()
                self.ensure_queue_exists(self._queue_name, durable=True)
                
                # Set prefetch_count if desired, to limit messages per consumer
                self._channel.basic_qos(prefetch_count=1) # Process one message at a time per consumer

                def wrapper_callback(ch, method, properties, body):
                    try:
                        message_data = json.loads(body.decode('utf-8'))
                        self._callback(message_data, ch, method, properties)
                    except json.JSONDecodeError as e:
                        print(f"Error decoding message body: {e} - Body: {body}")
                        ch.basic_nack(method.delivery_tag, requeue=False) # Don't requeue malformed messages
                    except Exception as e:
                        print(f"Error in consumer callback: {e}")
                        ch.basic_nack(method.delivery_tag, requeue=True)
                        
                self._channel.basic_consume(
                    queue=self._queue_name,
                    on_message_callback=wrapper_callback,
                    auto_ack=False
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

    def drain_queue_and_process(self, queue_name: str, process_message_func: Callable[[Dict, pika.adapters.blocking_connection.BlockingChannel, pika.spec.Basic.Deliver, pika.spec.BasicProperties], None]):
        """
        Connects to RabbitMQ, drains all available messages from the queue,
        processes them using process_message_func, and then closes the connection.
        Designed for one-shot batch jobs.
        """
        try:
            self._connect()
            self.ensure_queue_exists(queue_name, durable=True)
            
            # Set prefetch_count to allow processing multiple messages in memory before acking if desired
            # For a single job instance, this means it'll pull up to this many messages
            # For safety with a cron job that exits, process one by one and ack individually.
            self._channel.basic_qos(prefetch_count=1)

            print(f"Attempting to drain queue '{queue_name}'...")
            messages_processed = 0
            while True:
                method_frame, properties, body = self._channel.basic_get(queue=queue_name, auto_ack=False)
                if method_frame is None:
                    print("Queue is empty. No more messages to process.")
                    break # Queue is empty

                try:
                    message_data = json.loads(body.decode('utf-8'))
                    print(f" [x] Got message from queue: {message_data.get('interview_id')}")
                    process_message_func(message_data, self._channel, method_frame, properties)
                    messages_processed += 1
                except json.JSONDecodeError as e:
                    print(f" [!] Error decoding message body: {e} - Body: {body}. Nacking.")
                    self._channel.basic_nack(method_frame.delivery_tag, requeue=False)
                except Exception as e:
                    print(f" [!] Error processing message: {e}. Nacking for retry.")
                    self._channel.basic_nack(method_frame.delivery_tag, requeue=True)
            
            print(f"Drained and processed {messages_processed} messages from queue '{queue_name}'.")

        except pika.exceptions.AMQPConnectionError as e:
            print(f"Error connecting to RabbitMQ for queue draining: {e}")
            raise
        except Exception as e:
            print(f"Unhandled error during queue draining: {e}")
            raise
        finally:
            self._close_connection()


    def stop_consuming(self):
        self._is_consuming = False
        if self._channel:
            try:
                self._channel.stop_consuming()
                print("RabbitMQ consumer stopped.")
            except Exception as e:
                print(f"Error stopping RabbitMQ consumer: {e}")
        self._close_connection()
        if self._consumer_thread and self._consumer_thread.is_alive():
            self._consumer_thread.join(timeout=10)
            if self._consumer_thread.is_alive():
                print("Warning: Consumer thread did not terminate gracefully.")

    def _close_connection(self):
        if self._connection and self._connection.is_open:
            self._connection.close()