import json
import logging
import threading
import time
import signal
from datetime import datetime, timedelta

import pika
from matchengine.plugin_stub import DBSecrets
from matchminer.custom import run_ctims_matchengine_job


class RabbitMQMessage:
    def __init__(self):
        self.RABBITMQ_URI = None
        self.RABBITMQ_PORT = None
        self.SEND_QUEUE = None
        self.RECEIVE_QUEUE = None
        self.NIGHTLY_MATCH_STATUS_QUEUE = None
        self.receive_connection = None
        self.receive_channel = None
        self.send_connection = None
        self.send_channel = None

        # track the consumer thread for health monitoring
        self.consumer_thread = None
        # track if we need to shutdown the consumer, so we know to retry or not
        self.should_stop = False
        # track if consumer is actively listening (not just connected), for retry logic
        self.is_consuming = False

        # track when consumer last processed a message
        self.last_heartbeat = datetime.now()
        # count processed messages for monitoring
        self.message_count = 0

        self.initalize_rabbitmq()

    def initalize_rabbitmq(self):
        secrets = DBSecrets()
        rabbitmq_options = secrets.get_rabbitmq_connections()

        self.RABBITMQ_URI = rabbitmq_options["RABBITMQ_URI"]
        self.RABBITMQ_PORT = rabbitmq_options["RABBITMQ_PORT"]
        self.SEND_QUEUE = rabbitmq_options["SEND_QUEUE"]
        self.RECEIVE_QUEUE = rabbitmq_options["RECEIVE_QUEUE"]
        self.NIGHTLY_MATCH_STATUS_QUEUE = rabbitmq_options["NIGHTLY_MATCH_STATUS_QUEUE"]
        self.reconnect_rabbitmq()

    def reconnect_rabbitmq(self, max_retries=5, retry_delay=5):
        attempts = 0
        while attempts < max_retries:
            try:
                # Close existing connections first
                self._close_connections()

                # Connect to RabbitMQ receive queue with heartbeat of 5 minutes
                self.receive_connection = pika.BlockingConnection(pika.ConnectionParameters(
                    host=self.RABBITMQ_URI,
                    port=int(self.RABBITMQ_PORT),
                    heartbeat=5 * 60,
                    blocked_connection_timeout=5 * 60))
                self.receive_channel = self.receive_connection.channel()

                # Connect to RabbitMQ send queue
                self.send_connection = pika.BlockingConnection(pika.ConnectionParameters(
                    host=self.RABBITMQ_URI,
                    port=int(self.RABBITMQ_PORT),
                    heartbeat=5 * 60,
                    blocked_connection_timeout=5 * 60))
                self.send_channel = self.send_connection.channel()

                # Declare the queue
                self.receive_channel.queue_declare(queue=self.RECEIVE_QUEUE, durable=True)
                self.send_channel.queue_declare(queue=self.SEND_QUEUE, durable=True)
                self.send_channel.queue_declare(queue=self.NIGHTLY_MATCH_STATUS_QUEUE, durable=True)
                print("Connected to RabbitMQ")
                return True
            except Exception as e:
                # catch all exception instead of specific ones, so all exception goes to retry
                print(f"Error connecting to RabbitMQ attempt {attempts + 1}: {str(e)}")
                time.sleep(retry_delay)
                attempts += 1

        print(f"Failed to connect to RabbitMQ after {max_retries} attempts")
        return False

    # helper function to close the actual connection with try-catch
    def _close_connections(self):
        try:
            if self.receive_connection and not self.receive_connection.is_closed:
                self.receive_connection.close()
        except Exception as e:
            logging.warning(f"Error closing receive connection: {e}")

        try:
            if self.send_connection and not self.send_connection.is_closed:
                self.send_connection.close()
        except Exception as e:
            logging.warning(f"Error closing send connection: {e}")

    # Send message with a retry if can't publish, just retry once
    def send_message(self, message):
        try:
            self.send_channel.basic_publish(exchange="", routing_key=self.SEND_QUEUE, body=message)
            print(f" [x] Sent '{message}'")
        except Exception as e:
            logging.error(f"Error sending message: {e}")
            # Try to reconnect and resend once
            if self.reconnect_rabbitmq():
                try:
                    self.send_channel.basic_publish(exchange="", routing_key=self.SEND_QUEUE, body=message)
                    print(f" [x] Sent '{message}' after reconnection")
                except Exception as e2:
                    logging.error(f"Failed to send message after reconnection: {e2}")
                    # Don't retry infinitely

    # Send message with a retry if can't publish, just retry once
    def send_nightly_match_status_message(self, message):
        try:
            self.send_channel.basic_publish(exchange="", routing_key=self.NIGHTLY_MATCH_STATUS_QUEUE, body=message)
            print(f" [x] Sent nightly status '{message}'")
        except Exception as e:
            logging.error(f"Error sending nightly status message: {e}")
            if self.reconnect_rabbitmq():
                try:
                    self.send_channel.basic_publish(exchange="", routing_key=self.NIGHTLY_MATCH_STATUS_QUEUE, body=message)
                    print(f" [x] Sent nightly status '{message}' after reconnection")
                except Exception as e2:
                    logging.error(f"Failed to send nightly status message after reconnection: {e2}")

    def start_rabbit_consumer_thread(self):
        self.should_stop = False
        # start the retry start consumer thread
        self.consumer_thread = threading.Thread(target=self._consumer_with_monitoring, daemon=True)
        self.consumer_thread.start()

        # Setup signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)

    # clean shutdown without losing messages on system shutdowns
    def _signal_handler(self, signum, frame):
        print(f"Received signal {signum}, shutting down consumer...")
        self.should_stop = True
        if self.receive_channel and not self.receive_channel.is_closed:
            self.receive_channel.stop_consuming()

    # wrap the start_rabbit_consumer thread with retry logic
    def _consumer_with_monitoring(self):
        while not self.should_stop:
            try:
                self.start_rabbit_consumer()
                if not self.should_stop:  # Only log if not intentionally stopped
                    logging.error("Consumer exited unexpectedly, restarting in 10 seconds...")
                    time.sleep(10)
            except Exception as e:
                logging.error(f"Unexpected error in consumer monitoring: {e}")
                time.sleep(10)

    def start_rabbit_consumer(self, max_retries=5, retry_delay=5):
        attempts = 0
        while attempts < max_retries and not self.should_stop:
            try:
                # check we should be receiving to retry
                if not self.receive_connection or self.receive_connection.is_closed:
                    if not self.reconnect_rabbitmq():
                        attempts += 1
                        time.sleep(retry_delay)
                        continue

                self.receive_channel.basic_qos(prefetch_count=1)
                self.receive_channel.basic_consume(queue=self.RECEIVE_QUEUE, on_message_callback=self.process_job)

                print('Waiting for jobs...')

                # update state
                self.is_consuming = True
                self.last_heartbeat = datetime.now()

                # blocks and wait to consume, this returns when consumer is stopped or error
                self.receive_channel.start_consuming()

                # Indicates consumer has stopped (gracefully or due to error)
                self.is_consuming = False
                break

            except (pika.exceptions.AMQPConnectionError, ConnectionResetError,
                    pika.exceptions.StreamLostError, pika.exceptions.ChannelWrongStateError) as e:
                print(f"Connection error in consumer attempt {attempts + 1}: {str(e)}")
                self.is_consuming = False
                self._close_connections()
                attempts += 1
                time.sleep(retry_delay)

            except Exception as e:
                logging.error(f"Unexpected error in consumer: {e}")
                self.is_consuming = False
                self._close_connections()
                attempts += 1
                time.sleep(retry_delay)

        if attempts >= max_retries:
            logging.error(f"Consumer failed after {max_retries} attempts - will restart automatically")

    def process_job(self, ch, method, properties, body):
        try:
            # update state on job received
            self.last_heartbeat = datetime.now()
            self.message_count += 1

            # Process the job
            json_object = json.loads(body.decode())
            isNightlyRun = 'is_nightly_run' in json_object and json_object['is_nightly_run']

            if 'trial_internal_ids' in json_object:
                user_id = None
                if 'user_id' in json_object:
                    user_id = json_object['user_id']
                trial_internal_ids = json_object['trial_internal_ids']
                num_trials = len(trial_internal_ids)
                logging.info(f"Received job: {trial_internal_ids}")
                logging.info("running job")
                py_message_dict = {
                    "user_id": user_id,
                    "trial_internal_ids": trial_internal_ids,
                    "is_nightly_run": isNightlyRun,
                }
                try:
                    if isNightlyRun:
                        result = run_ctims_matchengine_job(trial_internal_ids, isNightlyRun=True)
                    else:
                        result = run_ctims_matchengine_job(trial_internal_ids, isNightlyRun=False)
                    num_failed_trials = len(result.keys())
                    failed_trial_internal_ids = list(result.keys())
                    if(num_failed_trials == 0):
                        # this is all success and no fail case
                        success_msg = f"Successfully ran job for trial internal ids {trial_internal_ids}"
                        py_message_dict.update({
                            "run_status": "SUCCESS",
                            "run_message": success_msg,
                            "failed_trial_internal_ids": failed_trial_internal_ids,
                        })
                    elif(num_failed_trials == num_trials):
                        # this is all fail case
                        error_msg = f"Error running job for trial internal ids {trial_internal_ids}"
                        py_message_dict.update({
                            "run_status": "FAIL",
                            "run_message": error_msg,
                            "failed_trial_internal_ids": failed_trial_internal_ids,
                        })
                    else:
                        # this is partial success and partial fail case
                        error_msg = f"Error running job for trial internal ids {trial_internal_ids}"
                        py_message_dict.update({
                            "run_status": "PARTIAL_SUCCESS",
                            "run_message": error_msg,
                            "failed_trial_internal_ids": failed_trial_internal_ids,
                        })
                except Exception as e:
                    error_msg = f"Error running job for trial internal ids {trial_internal_ids}: {str(e)}"
                    py_message_dict.update({
                        "run_status": "FAIL",
                        "run_message": error_msg
                    })
                finally:
                    json_error_msg = json.dumps(py_message_dict)
                    logging.error(json_error_msg)
                    try:
                        # always send the message to PMatch controller
                        self.send_nightly_match_status_message(json_error_msg)
                        if not isNightlyRun:
                            self.send_message(json_error_msg)
                    except Exception as e:
                        logging.error(f"Error sending message to queue: {str(e)}")
            else:
                error_msg = "Error: No trial_internal_ids in job"
                logging.error(error_msg)
                print(error_msg)
                try:
                    if isNightlyRun:
                        self.send_nightly_match_status_message(error_msg)
                    else:
                        self.send_message(error_msg)
                except Exception as e:
                    logging.error(f"Error sending error message: {e}")

        except Exception as e:
            logging.error(f"Critical error processing job: {e}")
        finally:
            # Acknowledge the job
            try:
                ch.basic_ack(delivery_tag=method.delivery_tag)
            except Exception as e:
                logging.error(f"Error acknowledging message: {e}")

    def close_rabbit_connection(self):
        print('Closing RabbitMQ connection...')

        self.should_stop = True
        if self.consumer_thread and self.consumer_thread.is_alive():
            # stop consuming and add timeout to ensure it exits
            self.consumer_thread.join(timeout=30)

        # Consistent cleanup logic with error handling
        self._close_connections()

    def __del__(self):
        self.close_rabbit_connection()
