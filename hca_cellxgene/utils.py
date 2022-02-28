import os
import json


def write_json_file(path: str, content: dict):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, 'w') as outfile:
        json.dump(content, outfile)


def read_json_file(path: str):
    with open(path) as json_file:
        return json.load(json_file)
