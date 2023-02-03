import json


def get_fields(data: str) -> str:
    data = json.loads(data)
    if isinstance(data, list):
        data = data[0]
    return data.keys()
