import os
from dotenv import load_dotenv

load_dotenv()

SECRET_KEY = os.getenv("SECRET_KEY", "default_secret_key")
RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "52.34.69.230")
RABBITMQ_PORT = os.getenv("RABBITMQ_PORT")
RABBITMQ_USERNAME = os.getenv("RABBITMQ_USERNAME")
RABBITMQ_PASSWORD = os.getenv("RABBITMQ_PASSWORD")
RABBITMQ_VHOST = os.getenv("RABBITMQ_VHOST")
DATABASE_URL = os.getenv("DATABASE_URL")
SENDGRID_SMTP = os.getenv("SENDGRID_SMTP")
SENDGRID_USER = os.getenv("SENDGRID_USER")
SENDGRID_PASS = os.getenv("SENDGRID_PASS")
BASE_URL = os.getenv("BASE_URL")
DEFAULT_FROM_EMAIL = os.getenv("DEFAULT_FROM_EMAIL")
