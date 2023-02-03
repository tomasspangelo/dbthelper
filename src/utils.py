import json


def get_fields(data: str) -> str:
    return json.loads(data).keys()
