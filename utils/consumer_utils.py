  
from datetime import datetime,timezone
import json
import os
# =============================
# data save in log files "transactions.log" and "processed_messages.json"
# =============================
def save_message(message_data: dict, event_type: str, processed_file: str, transaction_log_file: str):
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