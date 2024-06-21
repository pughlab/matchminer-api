#!/usr/bin/env python3
import argparse
import threading

from eve import Eve
from flask import redirect

from matchminer.elasticsearch import reset_elasticsearch
from matchminer.utilities import *
from matchminer.custom import blueprint
from matchminer import settings, security
from matchminer.events import register_hooks
from matchminer.validation import ConsentValidatorEve
from matchminer.components.oncore.oncore_app import oncore_blueprint
from matchminer.message.rabbitmq_message import RabbitMQMessage

logging.basicConfig(level=logging.INFO, format='[%(levelname)s] %(message)s', )

cur_dir = os.path.dirname(os.path.realpath(__file__))
static_dir = os.path.join(cur_dir, 'static')
settings_file = os.path.join(cur_dir, "matchminer/settings.py")

def on_fetched_resource(resource, response):
    for document in response['_items']:
        del(document['_created'])

if settings.NO_AUTH:
    logging.warning("NO AUTHENTICATION IS ENABLED - SKIPPING HIPAA LOGGING")
    app = Eve(settings=settings_file,
              static_folder=static_dir,
              static_url_path='',
              validator=ConsentValidatorEve)
else:
    app = Eve(settings=settings_file,
              static_folder=static_dir,
              static_url_path='',
              auth=security.TokenAuth,
              validator=ConsentValidatorEve)

app.config['SAML_PATH'] = os.path.join(cur_dir, 'saml')
app.config['SECRET_KEY'] = SAML_SECRET
app.register_blueprint(blueprint)
app.register_blueprint(oncore_blueprint)
app.on_fetched_resource += on_fetched_resource
register_hooks(app)


# Connect to RabbitMQ
# RABBITMQ_URI = os.getenv("RABBITMQ_URI")
# connection = pika.BlockingConnection(pika.ConnectionParameters(RABBITMQ_URI))
# channel = connection.channel()

# Declare the queue
# RECEIVE_QUEUE = os.getenv("RECEIVE_QUEUE")
# channel.queue_declare(queue=RECEIVE_QUEUE, durable=True)


# Define callback function for processing jobs
# def process_job(ch, method, properties, body):
#     # Process the job
#     json_object = json.loads(body.decode())
#     if 'trial_internal_ids' in json_object:
#         trial_internal_ids = json_object['trial_internal_ids']
#         print("Received job:", trial_internal_ids)
#         print("running job")
#         run_ctims_matchengine_job(trial_internal_ids)
#         # Acknowledge the job
#         ch.basic_ack(delivery_tag=method.delivery_tag)


# # Set up consumer
# channel.basic_qos(prefetch_count=1)  # Only one job at a time per consumer
# channel.basic_consume(queue='run_match', on_message_callback=process_job)


@app.after_request
def after_request(response):
    # dont use these headers because IE11 doesn't like them with fonts.
    if response.content_type != 'application/json':
        response.headers.add('Cache-Control', 'no-store, no-cache, must-revalidate, post-check=0, pre-check=0, max-age=0')
        response.headers.add('Pragma', 'no-cache')

    return response


@app.errorhandler(401)
def error_handle_401(err):
    return make_response("unauthorized access", 497)


@app.errorhandler(501)
def redirect_response(err):
    logging.info("redirected to: %s" % err)
    return make_response(redirect(err))


def run_server(args):
    os.environ['NO_AUTH'] = str(args.no_auth)

    # def start_rabbit_consumer():
    #     # Start consuming
    #     print('Waiting for jobs...')
    #     channel.start_consuming()
    #
    # def close_rabbit_connection():
    #     connection.close()
    #
    # atexit.register(close_rabbit_connection)

    rabbitmq_message = RabbitMQMessage()

    consumer_thread = threading.Thread(target=rabbitmq_message.start_rabbit_consumer)
    consumer_thread.start()

    app.run(host='0.0.0.0', port=settings.API_PORT,
            threaded=True,
            # use_reloader=True,
            debug=False
            )



# main
if __name__ == '__main__':
    main_p = argparse.ArgumentParser()
    subp = main_p.add_subparsers(help='sub-command help')

    subp_p = subp.add_parser('serve', help='runs webserver')
    subp_p.add_argument("-d", dest='debug', action='store_const', const=True, default=False)
    subp_p.add_argument("--no-auth", dest='no_auth', action='store_const', const=True,
                        default=False)
    subp_p.set_defaults(func=run_server)

    subp_p = subp.add_parser('reset-elasticsearch', help='resets elasticsearch')
    subp_p.set_defaults(func=lambda x: reset_elasticsearch())

    subp_p = subp.add_parser('reannotate-trials', help='regenerates elasticsearch fields on all trials')
    subp_p.set_defaults(func=lambda x: reannotate_trials())

    args = main_p.parse_args()
    args.func(args)
