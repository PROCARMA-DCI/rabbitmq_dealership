#!/usr/bin/env python3
"""
Send a test message to the RabbitMQ queue to verify consumer is working
"""


import pika
import json
from datetime import datetime

from config import (
    RABBITMQ_HOST,
    RABBITMQ_PASSWORD,
    RABBITMQ_PORT,
    RABBITMQ_VHOST,
    SECRET_KEY,
)


def send_test_message():
    # RabbitMQ connection
    credentials = pika.PlainCredentials("dealership_user", RABBITMQ_PASSWORD)
    parameters = pika.ConnectionParameters(
        host=RABBITMQ_HOST,
        port=RABBITMQ_PORT,
        virtual_host=RABBITMQ_VHOST,
        credentials=credentials,
    )

    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()

    # Ensure exchange and queue exist
    channel.exchange_declare(
        exchange="dealership_exchange", exchange_type="direct", durable=True
    )

    channel.queue_declare(queue="service_redemption_queue", durable=True)

    channel.queue_bind(
        exchange="dealership_exchange",
        queue="service_redemption_queue",
        routing_key="service.redemption",
    )

    # ✅ Requirement payload
    payload = {
        "request_type": "service_redemption",
        "apikey": "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJDb250cmFjdElEIjoiMTA3MzY5NDAiLCJJRCI6IjMiLCJDb3Vwb25JRCI6WyIxNDcyMzM2MCIsIjE0NzIzMzYzIl19.btlEAf3PGt7EZgI6g5l8r64LTxBJz1B0prfq1krqJew",
    }

    signed_message = payload

    # Publish message
    channel.basic_publish(
        exchange="dealership_exchange",
        routing_key="service.redemption",
        body=json.dumps(signed_message),
        properties=pika.BasicProperties(
            delivery_mode=2, content_type="application/json"  # Make message persistent
        ),
    )
    print(f"✓ Test message sent: {payload['apikey']}")
    print(f"   Service ID: {payload['request_type']}")

    # Check queue status
    method = channel.queue_declare(queue="service_redemption_queue", passive=True)
    message_count = method.method.message_count
    print(f"   Queue now has {message_count} message(s)")

    connection.close()
    print("✓ Test message sending completed")


if __name__ == "__main__":

    send_test_message()
