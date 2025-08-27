#!/usr/bin/env python3
"""
Service Redemption Consumer for Dealership System - NO DATABASE VERSION
Simple file-based logging only
"""
from utils.action.api_call import request_AUTTO, request_SOAP
from utils.action.db_query_call import (
    export_to_email,
    get_api_credentials,
    get_contract_details,
    get_coupons_details,
)
from utils.consumer_utils import save_message
import jwt
import pika
import json
import logging
import sys
import os

from typing import Dict, Any
import signal
import time

from config import (
    SECRET_KEY,
    RABBITMQ_HOST,
    RABBITMQ_PORT,
    RABBITMQ_USERNAME,
    RABBITMQ_PASSWORD,
    RABBITMQ_VHOST,
)


# Dynamically resolve log path
DEFAULT_LOG_FILENAME = "consumer.log"
DEFAULT_PROCESSED_FILE = "processed_messages.json"
DEFAULT_LOG_TRANSACTIONS = "transactions.log"

# Preferred: current script directory
current_dir = os.path.dirname(os.path.abspath(__file__))

log_file = os.path.join(current_dir, DEFAULT_LOG_FILENAME)
processed_file = os.path.join(current_dir, DEFAULT_PROCESSED_FILE)
transaction_log_file = os.path.join(current_dir, DEFAULT_LOG_TRANSACTIONS)

# Fallback if not writable
if not os.access(current_dir, os.W_OK):
    fallback_dir = "/tmp"  # Safe default for most Linux systems
    log_file = os.path.join(fallback_dir, DEFAULT_LOG_FILENAME)

# Now configure logging
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler(log_file), logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)

REQUIRED_FIELDS = ["apikey", "request_type"]


class ServiceRedemptionProcessor:
    def __init__(self):

        # RabbitMQ configuration
        self.rabbitmq_config = {
            "host": os.getenv("RABBITMQ_HOST", RABBITMQ_HOST),
            "port": int(os.getenv("RABBITMQ_PORT", RABBITMQ_PORT)),
            "username": os.getenv("RABBITMQ_USERNAME", RABBITMQ_USERNAME),
            "password": os.getenv("RABBITMQ_PASSWORD", RABBITMQ_PASSWORD),
            "virtual_host": os.getenv("RABBITMQ_VHOST", RABBITMQ_VHOST),
        }

        self.connection = None
        self.channel = None
        self.should_stop = False

        # Setup signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, self.signal_handler)
        signal.signal(signal.SIGTERM, self.signal_handler)

        logger.info("Service Redemption Processor initialized (No Database)")

    def signal_handler(self, signum, frame):
        logger.info(f"Received signal {signum}, shutting down gracefully...")
        self.should_stop = True
        if self.connection and not self.connection.is_closed:
            self.connection.close()

    def connect_rabbitmq(self):
        """Establish connection to RabbitMQ"""
        try:
            credentials = pika.PlainCredentials(
                self.rabbitmq_config["username"], self.rabbitmq_config["password"]
            )

            parameters = pika.ConnectionParameters(
                host=self.rabbitmq_config["host"],
                port=self.rabbitmq_config["port"],
                virtual_host=self.rabbitmq_config["virtual_host"],
                credentials=credentials,
                heartbeat=600,
                blocked_connection_timeout=300,
            )

            self.connection = pika.BlockingConnection(parameters)
            self.channel = self.connection.channel()

            # Declare exchange and queue (idempotent operations)
            self.channel.exchange_declare(
                exchange="dealership_exchange", exchange_type="direct", durable=True
            )

            self.channel.queue_declare(queue="service_redemption_queue", durable=True)

            self.channel.queue_bind(
                exchange="dealership_exchange",
                queue="service_redemption_queue",
                routing_key="service.redemption",
            )

            # Set QoS to process one message at a time
            self.channel.basic_qos(prefetch_count=1)

            logger.info("Successfully connected to RabbitMQ")
            return True

        except Exception as e:
            logger.error(f"Failed to connect to RabbitMQ: {str(e)}")
            return False

    # =============================
    # 🔑 JWT Decode & Validate
    # =============================
    def _decrypt_and_validate(self, payload: bytes) -> dict | None:
        try:
            logger.info(f"📩 Incoming body: {payload}")

            token = payload.get("apikey")  # now expecting JWT instead of apikey
            if not token:
                logger.warning("⚠️ Missing required 'token' field.")
                return None

            decoded = jwt.decode(token, SECRET_KEY, algorithms=["HS256"])

            logger.info(f"🔑 Decoded JWT payload: {decoded}")
            # ✅ If verification succeeds, return decoded data
            return decoded

        except jwt.ExpiredSignatureError:
            logger.error("❌ Token has expired.")
            return None
        except jwt.InvalidTokenError as e:
            logger.error(f"❌ Invalid token: {e}")
            return None
        except Exception as e:
            logger.error(f"❌ Error decrypting and validating message: {e}")
            return None

    # =============================
    # 📥 RabbitMQ Callback
    # =============================
    def message_callback(self, channel, method, properties, body):
        try:
            payload = json.loads(body)
            event_type = payload.get("request_type")
            # =========
            # Decrypt & Validate
            # =========
            message_data = self._decrypt_and_validate(payload)
            if not message_data:
                logger.warning("⚠️ Rejecting message due to failed JWT validation")
                channel.basic_ack(delivery_tag=method.delivery_tag)
                return

            ContractID = message_data.get("ContractID")
            coupon_ids = message_data.get("CouponID")
            ID = message_data.get("ID")
            # ========
            # DB OPERATION
            # ========
            contractDetails = get_contract_details(ContractID)
            coupansDetails = get_coupons_details(
                contract_id=ContractID, coupon_ids=coupon_ids
            )
            apiCredentials = get_api_credentials(ID)
            print(
                "contractDetails \n", json.dumps(contractDetails, indent=2, default=str)
            )
            print(
                "coupansDetails \n", json.dumps(coupansDetails, indent=2, default=str)
            )
            print(
                "apiCredentials \n", json.dumps(apiCredentials, indent=2, default=str)
            )
            # ========
            # requests
            # ========
            if contractDetails and coupansDetails and apiCredentials:
                result_autto = request_AUTTO(
                    contractDetails, coupansDetails, apiCredentials
                )
                print("AUTTO \n", json.dumps(result_autto, indent=2, default=str))

                export_to_email(contractDetails, ID)
                result_soap = request_SOAP(
                    apiCredentials, contractDetails, coupansDetails
                )
                print("SOAP \n", json.dumps(result_soap, indent=2, default=str))
            # ========
            # LOGGING
            # ========
            save_message(message_data, event_type, processed_file, transaction_log_file)

            # Always acknowledge for now
            channel.basic_ack(delivery_tag=method.delivery_tag)
            logger.info("✅ Message processed & acknowledged")

        except Exception as e:
            logger.error(f"❌ Error processing message: {e}")
            channel.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

        except json.JSONDecodeError as e:
            logger.error(f"❌ Invalid JSON in message: {e}")
            channel.basic_ack(delivery_tag=method.delivery_tag)
        except Exception as e:
            logger.error(f"❌ Error in message callback: {str(e)}")
            channel.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

    # =============================
    # ▶️ Start Consuming
    # =============================
    def start_consuming(self):
        """Start consuming messages from RabbitMQ"""
        logger.info("🚀 Starting service redemption consumer (No Database)...")

        while not self.should_stop:
            try:
                # Connect to RabbitMQ
                if not self.connect_rabbitmq():
                    logger.error(
                        "❌ Failed to connect to RabbitMQ, retrying in 5 seconds..."
                    )
                    time.sleep(5)
                    continue

                # Start consuming messages
                self.channel.basic_consume(
                    queue="service_redemption_queue",
                    on_message_callback=self.message_callback,
                )

                logger.info(
                    "👂 Consumer started. Waiting for messages. Press CTRL+C to stop."
                )
                self.channel.start_consuming()

            except KeyboardInterrupt:
                logger.info("🛑 Stopping consumer...")
                self.should_stop = True
                if self.channel:
                    self.channel.stop_consuming()
                break
            except Exception as e:
                logger.error(f"❌ Error in consumer loop: {str(e)}")
                # Clean up connection
                if self.connection and not self.connection.is_closed:
                    try:
                        self.connection.close()
                    except:
                        pass
                # Wait before retrying
                time.sleep(5)

        logger.info("🏁 Consumer stopped")


def main():
    """Main function to start the processor"""
    try:
        processor = ServiceRedemptionProcessor()
        processor.start_consuming()
    except Exception as e:
        logger.error(f"💥 Fatal error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
