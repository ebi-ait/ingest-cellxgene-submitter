import logging
import os
from typing import Union, Optional

import pandas as pd
import requests
from dotenv import load_dotenv
from ingest.api.ingestapi import IngestApi
from pandas import DataFrame

from hca_cellxgene.helpers.flat_chain import FlatChain
from hca_cellxgene.helpers.utils import get_nested


class Observation:
    def __init__(self, **kwargs):
        load_dotenv()
        self.fields = [
            'sample_id',
            'assay_ontology_term_id',
            'cell_type_ontology_term_id',
            'development_stage_ontology_term_id:human',
            'disease_ontology_term_id',
            'ethnicity_ontology_term_id:human',
            'is_primary_data',
            'organism_ontology_term_id',
            'sex_ontology_term_id',
            'tissue_ontology_term_id',
        ]

        for field in self.fields:
            self.__setattr__(field, kwargs.get(field))

    def to_data_frame(self) -> DataFrame:
        val_if_none = 'unknown'
        to_display = {key: self.__dict__.get(key, val_if_none) or val_if_none for key in self.fields}
        return pd.DataFrame(to_display, index=[0])


class IngestObservation(Observation):
    def __init__(self, cell_suspension_uuid, cell_type=None):
        self.cell_suspension_uuid = cell_suspension_uuid

        ingest_base = os.environ.get('INGEST_API', 'https://api.ingest.archive.data.humancellatlas.org/')
        self.ingest_api = IngestApi(ingest_base)

        self.__build_biomaterial_chain()

        # There must always be a donor and a specimen but cell line and organoid are optional nodes in tree
        cell_suspension = self.flat_chain.get_link('cell_suspension')
        specimen_from_organism = self.flat_chain.get_link('specimen_from_organism')
        donor_organism = self.flat_chain.get_link('donor_organism')
        lib_prep = self.flat_chain.get_link('library_preparation_protocol')

        diseases = specimen_from_organism['content']['diseases']
        if len(diseases) > 1:
            # ASSUMPTION: Use first disease, assuming all diseases are equal
            logging.warning(
                f'Biomaterial {specimen_from_organism["uuid"]["uuid"]} has multiple diseases. Using the first.'
            )

        data = {
            'sample_id': get_nested(cell_suspension, ['content', 'biomaterial_core', 'biomaterial_id']),
            'assay_ontology_term_id': None if not lib_prep else
            get_nested(lib_prep, ['content', 'library_construction_method', 'ontology']),
            'cell_type_ontology_term_id': cell_type,
            'development_stage_ontology_term_id:human':
                get_nested(donor_organism, ['content', 'development_stage', 'ontology']),
            'disease_ontology_term_id': get_nested(diseases, [0, 'ontology']),
            'ethnicity_ontology_term_id:human':
                get_nested(donor_organism, ['content', 'human_specific', 'ethnicity', 0, 'ontology']),
            'is_primary_data': True,
            'organism_ontology_term_id': get_nested(donor_organism, ['content', 'genus_species', 0, 'ontology']),
            'sex_ontology_term_id': IngestObservation.__get_sex_ontology_term(donor_organism),
            'tissue_ontology_term_id': self.__get_tissue_ontology_term()
        }

        super().__init__(**data)

    def set_cell_type(self, cell_type: str):
        # Convenience method to allow for setting of cell type after instantiation
        # Useful if have multiple cell suspensions with the same UUID and different cell types then
        # can de-deduplicate the creation of observations
        self.__setattr__('cell_type_ontology_term_id', cell_type)
        return self

    def __get_cell_suspension(self):
        result = self.ingest_api.get_entity_by_uuid('biomaterials', self.cell_suspension_uuid)
        if IngestObservation.__get_type_of_entity(result) != 'cell_suspension':
            raise TypeError("Is not a cell suspension")
        return result

    @staticmethod
    def __get_lib_prep_for_cell_suspension(cell_suspension) -> Optional[dict]:
        input_to = IngestObservation.__get_entities_from_link(cell_suspension, 'inputToProcesses')

        lib_prep = None
        for process in input_to:
            output_protocols = IngestObservation.__get_entities_from_link(process, 'protocols')
            lib_preps = [x for x in output_protocols if 'library_preparation_protocol' in x['content']['describedBy']]

            if len(lib_preps) > 1:
                logging.warning(
                    f"Process {process['uuid']['uuid']} should only have one library preparation protocol. "
                    f"Library preparation cannot be used and associated fields will be left blank in output."
                )
                return None

            if lib_prep and lib_preps[0]['uuid']['uuid'] != lib_prep['uuid']['uuid']:
                logging.warning(f"Cell suspension {cell_suspension['uuid']['uuid']} should only be associated to one "
                                f"library preparation protocol.")
                logging.warning(f"Using the first library preparation protocol with UUID {lib_prep['uuid']['uuid']}")

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

    def __add_entities_to_chain(self, entities_to_add: [dict], process_uuid: str, entity_generic_type: str) -> None:
        if len(entities_to_add) > 1:
            logging.warning(
                f'Process {process_uuid} has multiple '
                f'{entity_generic_type}s. Only using the first.'
            )

        to_add = entities_to_add[0]
        entity_type = IngestObservation.__get_type_of_entity(to_add)
        self.flat_chain.append(entity_type, to_add)
        logging.info(f'Added {entity_type} {to_add["uuid"]["uuid"]} to chain.')

    def __build_biomaterial_chain(self) -> None:
        logging.info(f'Building chain of biomaterials and protocols for cell suspension {self.cell_suspension_uuid}.')
        # Build linked list of biomaterial -> protocol (?) -> biomaterial from lib prep protocol to donor organism
        cell_suspension = self.__get_cell_suspension()
        lib_prep = IngestObservation.__get_lib_prep_for_cell_suspension(cell_suspension)

        # Can use a FlatChain since we know there can only be one entity of each entity_type in the chain
        if lib_prep:
            self.flat_chain = FlatChain(
                IngestObservation.__get_type_of_entity(lib_prep),
                lib_prep
            ).append(
                IngestObservation.__get_type_of_entity(cell_suspension),
                cell_suspension
            )
        else:
            self.flat_chain = FlatChain(
                IngestObservation.__get_type_of_entity(cell_suspension),
                cell_suspension
            )

        while self.flat_chain.current[0] != 'donor_organism':
            derived_by = IngestObservation.__get_entities_from_link(self.flat_chain.current[1], 'derivedByProcesses')
            if len(derived_by) > 1:
                # ASSUMPTION: a biomaterial can only be derived by one process
                logging.warning(
                    f'Biomaterial {self.flat_chain.current[1]["uuid"]["uuid"]} '
                    f'is derived by multiple processes. Only using the first.'
                )
            derived_by = derived_by[0]

            protocols_to_derive = IngestObservation.__get_entities_from_link(derived_by, 'protocols')
            biomaterials_to_derive = IngestObservation.__get_entities_from_link(derived_by, 'inputBiomaterials')

            if len(protocols_to_derive) > 0:
                # Protocols may or may not exist for deriving a given biomaterial
                self.__add_entities_to_chain(
                    protocols_to_derive, derived_by['uuid']['uuid'], 'protocol'
                )

            self.__add_entities_to_chain(
                biomaterials_to_derive, derived_by['uuid']['uuid'], 'biomaterial'
            )

    def __get_tissue_ontology_term(self) -> Optional[str]:
        to_try = ['organoid', 'cell_line', 'specimen_from_organism']
        for biomaterial_type in to_try:
            biomaterial = self.flat_chain.get_link(biomaterial_type)
            try:
                if biomaterial:
                    if biomaterial_type == to_try[0]:
                        if 'model_organ_part' in biomaterial['content']:
                            return biomaterial['content']['model_organ_part']['ontology']
                        return biomaterial['content']['model_organ']['ontology']
                    if biomaterial_type == to_try[1]:
                        return biomaterial['content']['tissue']['ontology']
                    if biomaterial_type == to_try[2]:
                        if 'organ_parts' in biomaterial['content']:
                            return biomaterial['content']['organ_parts'][0]['ontology']
                        return biomaterial['content']['organ']['ontology']
            except KeyError as e:
                logging.warning(f'Failed to get tissue_ontology_term_id for {biomaterial_type} with uuid '
                                f'{biomaterial["uuid"]["uuid"]}')
                logging.warning(f'ERROR: {e}')
        return None

    @staticmethod
    def __get_sex_ontology_term(donor_organism) -> Optional[str]:
        sex = get_nested(donor_organism, ['content', 'sex'])
        if sex == 'male':
            return 'PATO:0000384'
        if sex == 'female':
            return 'PATO:0000383'
        return None
