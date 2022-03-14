import json
import os
import uuid
from contextlib import contextmanager
from pathlib import Path
from urllib.parse import urlparse

import requests
from dotenv import load_dotenv
from tqdm import tqdm

from hca_cellxgene import context

load_dotenv()


def write_json_file(path: str, content: dict) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, 'w') as outfile:
        json.dump(content, outfile)


def read_json_file(path: str) -> dict:
    with open(path) as json_file:
        return json.load(json_file)


@contextmanager
def download_file(url) -> os.PathLike:
    filename = str(uuid.uuid4()) + os.path.basename(urlparse(url).path)
    download_dir = Path(context['wd'], 'downloads')
    download_dir.mkdir(parents=True)
    download_path = Path(download_dir, filename)

    # Streaming, so we can iterate over the response.
    response = requests.get(url, stream=True)
    response.raise_for_status()
    total_size_in_bytes = int(response.headers.get('content-length', 0))
    block_size = 1024  # 1 Kibibyte
    progress_bar = tqdm(total=total_size_in_bytes, unit='iB', unit_scale=True)

    f = open(download_path, 'wb+')
    try:
        for data in response.iter_content(block_size):
            progress_bar.update(len(data))
            f.write(data)

        if total_size_in_bytes != 0 and progress_bar.n != total_size_in_bytes:
            raise Exception(f"Something went wrong downloading file from {url}")

        yield Path(download_path)
    finally:
        f.close()
        progress_bar.close()


def get_nested(d: dict, list_of_keys: [str], default=None):
    for k in list_of_keys:
        if k not in d and not isinstance(k, int):
            return default
        d = d[k]
    return d
