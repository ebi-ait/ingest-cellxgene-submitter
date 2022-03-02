import json
import logging
import os
from typing import Generator

import requests
from dotenv import load_dotenv
# from ingest.api.ingestapi import IngestApi

import utils

load_dotenv()


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
        if fetch_result['Status'] != 302 or fetch_result['Status'] != 301:
            raise requests.exceptions.RequestException(
                f'Unexpected status code when getting download link for {self.uuid}')

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
        link = 'https://service.azul.data.humancellatlas.org/fetch/repository/files'
        r = requests.get(link, params={
            'catalog': os.environ.get('DCP_VERSION')
        })
        r.raise_for_status()
        return self.__get_azul_file_location(r.json())

    def get_azul_project_metadata_tsv(self):
        with utils.download_file(self._get_azul_metadata_download_link()) as file:
            return file

    def get_contributor_generated_matrix(self):
        with utils.download_file(self._get_azul_contributor_matrix_download_link()) as file:
            return file
