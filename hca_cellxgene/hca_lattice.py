import logging
import os
from typing import Generator

from ingest.api.ingestapi import IngestApi
from json_converter.json_mapper import JsonMapper

import hca_cellxgene.helpers.utils as utils


class HcaToLattice:
    def __init__(self, output_dir: str):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.logger.setLevel(logging.INFO)
        self.ingest_api = IngestApi('http://localhost:8080')
        self.spec_dir = os.path.join(os.path.dirname(__file__), 'specifications')
        self.output_dir = output_dir

    @staticmethod
    def __get_schema_type(hca_document: dict):
        return hca_document['describedBy'].split('/')[-1]

    @staticmethod
    def __get_lattice_schema_name(hca_schema_type: str):
        mappings = {
            'donor_organism': 'HumanPostnatalDonor'
        }

        return mappings[hca_schema_type]

    def __convert(self, hca_document: dict):
        spec = utils.read_json_file(
            os.path.join(self.spec_dir, HcaToLattice.__get_schema_type(hca_document) + '.json')
        )
        return JsonMapper(hca_document).map(spec)

    @staticmethod
    def __get_entity_short_name(entity: dict):
        try:
            schema_type = entity['content']['schema_type']
            return entity['content'][f'{schema_type}_core'][f'{schema_type}_id']
        except Exception:
            raise TypeError('Failed to get short name (id) for entity')

    def __convert_submission(self, submission: dict) -> Generator[tuple[str, str, str, dict], None, None]:
        sub_uuid = submission['uuid']['uuid']
        self.logger.info(f'Converting submission with UUID {sub_uuid}...')

        to_convert = ['biomaterials', 'protocols', 'processes', 'files']
        for entity_type in to_convert:
            entities = self.ingest_api.get_entities(submission['_links']['self']['href'], entity_type)
            for entity in entities:
                yield (
                    sub_uuid,
                    self.__get_lattice_schema_name(self.__get_schema_type(entity['content'])),
                    self.__get_entity_short_name(entity),
                    self.__convert(entity['content'])
                )

        self.logger.info(f'Done converting submission with UUID {sub_uuid}')

    def convert_project(self, uuid: str) -> Generator[tuple[str, str, str, dict], None, None]:
        self.logger.info(f'Converting project with UUID {uuid}...')

        project = self.ingest_api.get_project_by_uuid(uuid)
        submissions = self.ingest_api.get(project['_links']['submissionEnvelopes']['href']).json()['_embedded']['submissionEnvelopes']

        for submission in submissions:
            for result in self.__convert_submission(submission):
                yield result

        self.logger.info(f'Done converting project with UUID {uuid}')

