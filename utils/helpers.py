import json


def Print(data, title="Title"):
    print(f"{title} \n", json.dumps(data, indent=2, default=str))
