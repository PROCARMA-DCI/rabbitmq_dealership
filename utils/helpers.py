from datetime import datetime
import json


def Print(data, title="Title"):
    formatted = json.dumps(data, indent=2, default=str)
    print(f"{title}\n{formatted}")


def formatDate(date: int) -> str:
    return datetime.fromtimestamp(int(date)).strftime("%m-%d-%Y")
