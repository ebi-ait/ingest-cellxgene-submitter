import os
from typing import Union

import pandas as pd
import requests
from dotenv import load_dotenv
from ingest.api.ingestapi import IngestApi
from pandas import DataFrame



class Observation:
    def __init__(self, data):
        load_dotenv()
        self.data = data
        fields = [
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

        # for field in fields:
        #     self[field] = kwargs.get(field, 'UNSET')

    def __setitem__(self, key, value):
        self[key] = value


    @staticmethod
    def __get_entity_from_link(entity: dict, link, allow_multiple=False) -> Union[list[dict], dict]:
        r = requests.get(
            entity['_links'][link]['href']
        )

        r.raise_for_status()
        link_result = r.json()

        result_list = list(link_result['_embedded'].values())[0]

        if not allow_multiple:
            entity_type = entity['content']['schema_type']
            uuid = entity['uuid']['uuid']

            if len(result_list) > 1:
                raise IndexError(f'{entity_type} "{uuid}" should only return one entity from link "{link}".')

            return result_list[0]

        return result_list

    @staticmethod
    def __get_tree_to_donor(cell_suspension: dict):
        tree = {}
        cur_node = cell_suspension
        while 1:
            derived_by = Observation.__get_entity_from_link(cur_node, 'derivedByProcesses')
            # ASSUMPTION: Just take first, could have multiple inputs
            # (e.g. cell suspension may have multiple organoids) but they should share properties we care about
            biomaterial = Observation.__get_entity_from_link(derived_by, 'inputBiomaterials', True)[0]

            biomaterial_type = biomaterial['content']['describedBy'].split('/')[-1]
            tree[biomaterial_type] = biomaterial

            print(biomaterial_type)

            if biomaterial_type == 'donor_organism':
                break

            cur_node = biomaterial

        return tree


    @staticmethod
    def __compute_tissue_ontology_term(cell_suspension_tree):
        keys = cell_suspension_tree.keys()
        if 'organoid' in keys:
            return  cell_suspension_tree['organoid']['content']['model_organ_part']['text']
        if 'cell_line' in keys:
            return cell_suspension_tree['cell_line']['content']['tissue']['text']
        if 'specimen_from_organism' in keys:
            return cell_suspension_tree['specimen_from_organism']['content']['organ_parts']['text']


    @staticmethod
    def from_hca_cell_suspension(biomaterial_uuid: str) -> 'Observation':
        ingest_base = os.environ.get('INGEST_API')
        print(ingest_base)
        ingest_api = IngestApi(ingest_base)

        # TODO refector to make a full tree structure with childs etc. Maybe use linked list
        cell_suspension = ingest_api.get_entity_by_uuid('biomaterials', biomaterial_uuid)

        # get library preparation protocol
        input_to = Observation.__get_entity_from_link(cell_suspension, 'inputToProcesses', True)
        lib_prep = None
        for process in input_to:
            output_protocols = Observation.__get_entity_from_link(process, 'protocols', True)
            lib_preps = [x for x in output_protocols if 'library_preparation_protocol' in x['content']['describedBy']]

            if len(lib_preps) > 1:
                raise IndexError("Should only have one library preparation protocol per process.")

            if not lib_prep:
                lib_prep = lib_preps[0]
            elif lib_preps[0]['uuid']['uuid'] != lib_prep['uuid']['uuid']:
                raise TypeError("Cell suspension should only be associated to one library preparation protocol.")

        input_tree = Observation.__get_tree_to_donor(cell_suspension)

        # There must always be a donor and a specimen but cell line and organoid are optional parents
        specimen_from_organism = input_tree['specimen_from_organism']
        donor_organism = input_tree['donor_organism']

        data = {
            'assay_ontology_term_id': lib_prep['content']['library_construction_method']['text'],
            'development_stage_ontology_term_id:human': donor_organism['content']['development_stage']['text'],
            # ASSUMPTION: Use first disease, assuming all diseases are equal
            'disease_ontology_term_id': specimen_from_organism['content']['diseases'][0]['text'],
            'ethnicity_ontology_term_id:human': donor_organism['content']['human_specific']['ethnicity'][0]['text'],
            'is_primary_data': True,
            'organism_ontology_term_id': donor_organism['content']['genus_species'][0]['text'],
            'sex_ontology_term_id': donor_organism['content']['sex'],
            'tissue_ontology_term_id': Observation.__compute_tissue_ontology_term(specimen_from_organism)
        }

        return Observation(data)

    def to_data_frame(self) -> DataFrame:
        return pd.DataFrame(self.data, index=[0])
