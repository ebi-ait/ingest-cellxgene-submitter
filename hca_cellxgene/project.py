import json
import logging
import os
import tarfile
from pathlib import Path
from typing import Generator

import requests
from dotenv import load_dotenv

from hca_cellxgene import context
from hca_cellxgene.helpers import utils

# from ingest.api.ingestapi import IngestApi

load_dotenv()


# THIS IS PROBABLY NOT GOING TO BE USED NOW
# Was created before when trying to go directly from project uuid to h5ad
# Now we believe we can only go from cell_suspension uuid to h5ad
# Leaving here until 100% sure we cannot go from project uuid to h5ad

class Project:
    def __init__(self, uuid: str):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.logger.setLevel(logging.INFO)
        # self.ingest_api = IngestApi(os.environ.get('INGEST_API', 'http://localhost:8080'))
        self.uuid = uuid
        self.project = None
        # self.refresh()

    def get_submissions(self) -> Generator[dict, None, None]:
        link = self.project['_links']['submissionEnvelopes']['href']
        for submission in self.ingest_api.get(link).json()['_embedded']['submissionEnvelopes']:
            yield submission

    def refresh(self) -> None:
        pass
        # self.project = self.ingest_api.get_project_by_uuid(self.uuid)

    def __get_azul_file_location(self, fetch_result: dict):
        if fetch_result['Status'] != 302 and fetch_result['Status'] != 301:
            raise requests.exceptions.RequestException(
                f'Unexpected status code when getting download link for {self.uuid}. Was {fetch_result["Status"]}'
            )

        return fetch_result['Location']

    def _get_azul_metadata_download_link(self):
        link = 'https://service.azul.data.humancellatlas.org/fetch/manifest/files'
        r = requests.get(link, params={
            'catalog': os.environ.get('DCP_VERSION'),
            'format': 'compact',
            'filters': json.dumps({
                'projectId': {'is': [self.uuid]}
            })
        })
        r.raise_for_status()
        return self.__get_azul_file_location(r.json())

    def _get_azul_contributor_matrix_download_link(self):
        link = f'https://service.azul.data.humancellatlas.org/fetch/repository/files/{self.uuid}'
        r = requests.get(link, params={
            'catalog': os.environ.get('DCP_VERSION')
        })
        r.raise_for_status()
        return self.__get_azul_file_location(r.json())

    def get_project_metadata(self) -> os.PathLike:
        with utils.download_file(self._get_azul_metadata_download_link()) as file_path:
            return file_path

    def get_contributor_generated_matrices_and_barcodes(self) -> ([os.PathLike], [os.PathLike]):
        with utils.download_file(self._get_azul_contributor_matrix_download_link()) as file_path:
            tar = tarfile.open(file_path)
            extract_dir = Path(context['wd'], 'extracted', self.uuid)
            extract_dir.mkdir(parents=True, exist_ok=True)
            tar.extractall(path=extract_dir)

            matrix_barcode_combinations = []

            for matrix in extract_dir.iterdir():
                if not matrix.is_file():
                    raise TypeError("Expected tar file of contributor matrices to only contain files")

                if matrix.suffixes == ['mtx', 'gz']:
                    root_name = matrix.stem.replace('_matrix.mtx.gz', '')

                    for barcode in extract_dir.iterdir():
                        if barcode.suffixes == ['tsv', 'gz'] \
                                and "barcodes" in barcode.stem \
                                and root_name in barcode.stem:
                            matrix_barcode_combinations.append((matrix, barcode))

            if len(matrix_barcode_combinations) < 1:
                raise IndexError("Expected at least one matrix and barcode combination with "
                                 "extensions of \".mtx.gz\" and \"_barcode.tsv.gz\", respectively.")

            return matrix_barcode_combinations


