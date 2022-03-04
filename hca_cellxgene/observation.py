import logging
import os
from typing import Union, Optional

import pandas as pd
import requests
from dotenv import load_dotenv
from ingest.api.ingestapi import IngestApi
from pandas import DataFrame


class Observation:
    def __init__(self, **kwargs):
        load_dotenv()
        self.fields = [
            'assay_ontology_term_id',
            # Ignoring the below for now: talk to Wei
            # 'cell_type_ontology_term_id',
            'development_stage_ontology_term_id:human',
            'disease_ontology_term_id',
            'ethnicity_ontology_term_id:human',
            'is_primary_data',
            'organism_ontology_term_id',
            'sex_ontology_term_id',
            'tissue_ontology_term_id',
        ]

        for field in self.fields:
            self.__setattr__(field, kwargs.get(field, 'UNSET') or 'UNSET')

    def to_data_frame(self) -> DataFrame:
        to_display = {key: self.__dict__[key] for key in self.fields}
        return pd.DataFrame(to_display, index=[0])


class IngestObservation(Observation):
    def __init__(self, cell_suspension_uuid):
        self.cell_suspension_uuid = cell_suspension_uuid

        ingest_base = os.environ.get('INGEST_API')
        self.ingest_api = IngestApi(ingest_base)

        self.__build_biomaterial_chain()

        # There must always be a donor and a specimen but cell line and organoid are optional parents
        specimen_from_organism = self.__get_entity_with_type('specimen_from_organism')
        donor_organism = self.__get_entity_with_type('donor_organism')
        lib_prep = self.__get_entity_with_type('library_preparation_protocol')

        diseases = specimen_from_organism['content']['diseases']
        if len(diseases) > 1:
            # ASSUMPTION: Use first disease, assuming all diseases are equal
            logging.warning(
                f'Biomaterial {specimen_from_organism["uuid"]["uuid"]} has multiple diseases. Using the first.'
            )

        super().__init__(**{
            'assay_ontology_term_id': lib_prep['content']['library_construction_method']['text'],
            'development_stage_ontology_term_id:human': donor_organism['content']['development_stage']['text'],
            'disease_ontology_term_id': diseases[0]['text'],
            'ethnicity_ontology_term_id:human': donor_organism['content']['human_specific']['ethnicity'][0]['text'],
            'is_primary_data': True,
            'organism_ontology_term_id': donor_organism['content']['genus_species'][0]['text'],
            'sex_ontology_term_id': donor_organism['content']['sex'],
            'tissue_ontology_term_id': self.__get_tissue_ontology_term()
        })

    def __get_cell_suspension(self):
        return self.ingest_api.get_entity_by_uuid('biomaterials', self.cell_suspension_uuid)

    @staticmethod
    def __get_lib_prep_for_cell_suspension(cell_suspension) -> dict:
        input_to = IngestObservation.__get_entities_from_link(cell_suspension, 'inputToProcesses')

        lib_prep = None
        for process in input_to:
            output_protocols = IngestObservation.__get_entities_from_link(process, 'protocols')
            lib_preps = [x for x in output_protocols if 'library_preparation_protocol' in x['content']['describedBy']]

            if len(lib_preps) > 1:
                raise IndexError(
                    f"Process {process['uuid']['uuid']} should only have one library preparation protocol."
                )

            if lib_prep and lib_preps[0]['uuid']['uuid'] != lib_prep['uuid']['uuid']:
                raise TypeError("Cell suspension should only be associated to one library preparation protocol.")

            lib_prep = lib_preps[0]
        return lib_prep

    @staticmethod
    def __get_entities_from_link(entity: dict, link: str) -> Union[list[dict], dict]:
        logging.info(f'Getting {link} for {IngestObservation.__get_type_of_entity(entity)} {entity["uuid"]["uuid"]}')

        r = requests.get(
            entity['_links'][link]['href']
        )

        r.raise_for_status()
        link_result = r.json()

        return list(link_result['_embedded'].values())[0]

    @staticmethod
    def __get_type_of_entity(entity: dict) -> str:
        return entity['content']['describedBy'].split('/')[-1]

    def __build_biomaterial_chain(self) -> None:
        logging.info(f'Building chain of biomaterials and protocols for cell suspension {self.cell_suspension_uuid}.')
        # Build linked list of biomaterial -> protocol (?) -> biomaterial from lib prep protocol to donor organism
        cell_suspension = self.__get_cell_suspension()
        lib_prep = IngestObservation.__get_lib_prep_for_cell_suspension(cell_suspension)
        lib_prep['child'] = cell_suspension

        self.chain = lib_prep

        cur_leaf = self.chain['child']  # cell_suspension
        while 1:
            derived_by = IngestObservation.__get_entities_from_link(cur_leaf, 'derivedByProcesses')
            if len(derived_by) > 1:
                # ASSUMPTION: a biomaterial can only be derived by one process
                logging.warning(
                    f'Biomaterial {cur_leaf["uuid"]["uuid"]} is derived by multiple processes. Only using the first.'
                )
            derived_by = derived_by[0]

            protocols_to_derive = IngestObservation.__get_entities_from_link(derived_by, 'protocols')
            biomaterials_to_derive = IngestObservation.__get_entities_from_link(derived_by, 'inputBiomaterials')

            if len(protocols_to_derive) > 0:
                # Protocols may or may not exist for deriving a given biomaterial
                if len(protocols_to_derive) > 1:
                    # ASSUMPTION: a biomaterial can only be derived by one protocol
                    logging.warning(
                        f'Process {derived_by["uuid"]["uuid"]} has multiple protocols. Only using the first.'
                    )

                cur_leaf['child'] = protocols_to_derive[0]
                cur_leaf = cur_leaf['child']
                logging.info(
                    f'Added {IngestObservation.__get_type_of_entity(cur_leaf)} {cur_leaf["uuid"]["uuid"]} to chain.'
                )

            # (e.g. cell suspension may have multiple organoids) but they should share properties we care about
            if len(biomaterials_to_derive) > 1:
                # ASSUMPTION: A biomaterial can only be derived by one biomaterial
                logging.warning(
                    f'Process {derived_by["uuid"]["uuid"]} has multiple biomaterials. Only using the first.'
                )

            cur_leaf['child'] = biomaterials_to_derive[0]
            cur_leaf = cur_leaf['child']
            logging.info(
                f'Added {IngestObservation.__get_type_of_entity(cur_leaf)} {cur_leaf["uuid"]["uuid"]} to chain.'
            )

            if IngestObservation.__get_type_of_entity(cur_leaf) == 'donor_organism':
                break

    def __get_tissue_ontology_term(self) -> Optional[str]:
        to_try = ['organoid', 'cell_line', 'specimen_from_organism']
        try:
            for biomaterial_type in to_try:
                biomaterial = self.__get_entity_with_type(biomaterial_type)
                if biomaterial and biomaterial_type == to_try[0]:
                    return biomaterial['content']['model_organ_part']['text']
                if biomaterial and biomaterial_type == to_try[1]:
                    return biomaterial['content']['tissue']['text']
                if biomaterial and biomaterial_type == to_try[2]:
                    return biomaterial['content']['organ_parts']['text']
                return None
        except KeyError:
            return None

    def __get_entity_with_type(self, entity_type: str) -> Optional[dict]:
        cur_leaf = self.chain
        while 1:
            if IngestObservation.__get_type_of_entity(cur_leaf) == entity_type:
                return cur_leaf
            if 'child' not in cur_leaf:
                return None
            cur_leaf = cur_leaf['child']
