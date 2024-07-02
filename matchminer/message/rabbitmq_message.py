import json
from datetime import time

import pika
from matchengine.plugin_stub import DBSecrets
from matchminer.custom import run_ctims_matchengine_job


class RabbitMQMessage:
    def __init__(self):
        self.RABBITMQ_URI = None
        self.RABBITMQ_PORT = None
        self.SEND_QUEUE = None
        self.RECEIVE_QUEUE = None
        self.receive_connection = None
        self.receive_channel = None
        self.send_connection = None
        self.send_channel = None
        self.initalize_rabbitmq()


    def initalize_rabbitmq(self):
        secrets = DBSecrets()
        rabbitmq_options = secrets.get_rabbitmq_connections()

        self.RABBITMQ_URI = rabbitmq_options["RABBITMQ_URI"]
        self.RABBITMQ_PORT = rabbitmq_options["RABBITMQ_PORT"]
        self.SEND_QUEUE = rabbitmq_options["SEND_QUEUE"]
        self.RECEIVE_QUEUE = rabbitmq_options["RECEIVE_QUEUE"]
        self.reconnect_rabbitmq()

    def reconnect_rabbitmq(self, max_retries=5, retry_delay=5):
        for attempt in range(max_retries):
            try:
                # Connect to RabbitMQ receive queue
                self.receive_connection = pika.BlockingConnection(pika.ConnectionParameters(
                    host=self.RABBITMQ_URI,
                    port=int(self.RABBITMQ_PORT),
                    heartbeat=8 * 60,
                    blocked_connection_timeout=8 * 60))
                self.receive_channel = self.receive_connection.channel()

                # Connect to RabbitMQ send queue
                self.send_connection = pika.BlockingConnection(pika.ConnectionParameters(
                    host=self.RABBITMQ_URI,
                    port=int(self.RABBITMQ_PORT),
                    heartbeat=8 * 60,
                    blocked_connection_timeout=8 * 60))
                self.send_channel = self.send_connection.channel()

                # Declare the queue
                self.receive_channel.queue_declare(queue=self.RECEIVE_QUEUE, durable=True)
                self.send_channel.queue_declare(queue=self.SEND_QUEUE, durable=True)
                print("Connected to RabbitMQ")
                break
            except pika.exceptions.AMQPConnectionError as e:
                print(f"Error connecting to RabbitMQ attempt: {attempt + 1}: {str(e)}")
                time.sleep(retry_delay)
            except ConnectionResetError as e:
                print(f"XConnecting reset attempt: {attempt + 1}: {str(e)}")
                time.sleep(retry_delay)
        else:
            print(f"Failed to connect to RabbitMQ after {max_retries} attempts")


    def send_message(self, message):
        self.send_channel.basic_publish(exchange="", routing_key=self.SEND_QUEUE, body=message)
        print(f" [x] Sent '{message}'")


    def start_rabbit_consumer(self):
        self.receive_channel.basic_qos(prefetch_count=1)  # Only one job at a time per consumer
        self.receive_channel.basic_consume(queue=self.RECEIVE_QUEUE, on_message_callback=self.process_job)

        # Start consuming
        print('Waiting for jobs...')
        self.receive_channel.start_consuming()


    def process_job(self, ch, method, properties, body):
        # Process the job
        json_object = json.loads(body.decode())

        # Acknowledge the job
        ch.basic_ack(delivery_tag=method.delivery_tag)

        if 'trial_internal_ids' in json_object:
            trial_internal_ids = json_object['trial_internal_ids']
            print("Received job:", trial_internal_ids)
            print("running job")
            try:
                run_ctims_matchengine_job(trial_internal_ids)
            except Exception as e:
                error_msg = f"Error running job for trial internal ids {trial_internal_ids}: {str(e)}"
                print(error_msg)
                self.send_message(error_msg)
            else:
                success_msg = f"Successfully ran job for trial internal ids {trial_internal_ids}"
                print(success_msg)
                self.send_message(success_msg)
        else:
            error_msg = "Error: No trial_internal_ids in job"
            print(error_msg)
            self.send_message(error_msg)

    def close_rabbit_connection(self):
        print('Closing RabbitMQ connection...')
        self.receive_connection.close()


    def __del__(self):
        self.close_rabbit_connection()
