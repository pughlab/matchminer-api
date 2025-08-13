from flask import Blueprint, jsonify
from matchminer.message.rabbitmq_message import RabbitMQMessage

class RabbitMQFactory:
    _instance = None

    @classmethod
    def get_instance(cls):
        if cls._instance is None:
            cls._instance = RabbitMQMessage()
        return cls._instance


rabbitmq_blueprint = Blueprint('rabbitmq_health', __name__)
rabbitmq = RabbitMQFactory.get_instance()

@rabbitmq_blueprint.route("/health/rabbitmq", methods=["GET"])
def rabbitmq_health():
    result = rabbitmq.health_check()
    status_code = 200 if result["status"] else 503
    return jsonify(result), status_code