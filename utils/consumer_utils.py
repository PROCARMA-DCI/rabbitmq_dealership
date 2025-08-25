from datetime import datetime, timezone
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import json
import os
import smtplib

from config import DEFAULT_FROM_EMAIL, SENDGRID_PASS, SENDGRID_SMTP, SENDGRID_USER


# =============================
# data save in log files "transactions.log" and "processed_messages.json"
# =============================
def save_message(
    message_data: dict, event_type: str, processed_file: str, transaction_log_file: str
):
    # Use consistent UTC ISO timestamp
    utc_date = datetime.now(timezone.utc).isoformat()

    # --- Save into processed_file as JSON array ---
    existing_data = []
    if os.path.exists(processed_file):
        with open(processed_file, "r") as f:
            try:
                existing_data = json.load(f)
            except json.JSONDecodeError:
                existing_data = []

    # Add UTC date to message
    message_data["date"] = utc_date
    existing_data.append(message_data)

    with open(processed_file, "w") as f:
        json.dump(existing_data, f, indent=2)

    # --- Save into transaction_log_file with UTC time ---
    with open(transaction_log_file, "a") as f:
        f.write(f"{utc_date} - {event_type} - {json.dumps(message_data)}\n")


def send_email(to_email: str, subject: str, html_content: str) -> bool:
    print(f"{to_email} | Following Message Sent By Customer:\n{html_content}")
    """Send email via SendGrid SMTP"""
    try:
        msg = MIMEMultipart()
        msg["From"] = DEFAULT_FROM_EMAIL
        msg["To"] = "sbshamail123@gmail.com"
        msg["Subject"] = subject
        msg.attach(MIMEText(html_content, "html"))

        with smtplib.SMTP(SENDGRID_SMTP, 587) as server:
            server.starttls()
            server.login(SENDGRID_USER, SENDGRID_PASS)
            server.send_message(msg)
        return True
    except Exception as e:
        print(f"❌ Email sending failed: {e}")
        return False
