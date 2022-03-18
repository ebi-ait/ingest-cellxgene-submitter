import logging
import os
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import anndata as ad
import pandas as pd
import scipy.io as sio
from dotenv import load_dotenv
from pandas import SparseDtype, DataFrame

from hca_cellxgene.observation import IngestObservation

load_dotenv()


def __load_barcodes(barcode_file_path) -> []:
    return pd.read_csv(filepath_or_buffer=barcode_file_path, header=None)[0].array


def __load_matrix(matrix_file_path) -> SparseDtype:
    with open(matrix_file_path) as file:
        matrix_file = sio.mmread(file)
        # Use a sparse data frame since most values are 0 and it's more efficient
        matrix = pd.DataFrame.sparse.from_spmatrix(matrix_file)
        return matrix.transpose()


def __build_obs_row(uuid_and_type: (str, str)) -> (str, DataFrame):
    cell_suspension_uuid, cell_type = uuid_and_type
    logging.info(f'building obs row for {cell_suspension_uuid}')
    return cell_suspension_uuid, IngestObservation(cell_suspension_uuid, cell_type).to_data_frame()


def __build_obs(inputs: [(str, str)]) -> DataFrame:
    # Build the observation layer only for unique uuids, not for each row as each uuid may be duplicated multiple times
    # Saves network requests
    unique_inputs = set(inputs)
    with ThreadPoolExecutor() as executor:
        unique_obs_rows = executor.map(__build_obs_row, unique_inputs)

    # Create a hashmap for convenience in later lookup
    unique_obs_hashmap = {x[0]: x[1] for x in unique_obs_rows}

    # Go through each row in the original file and get the created observations
    total_obs_rows = []
    for uuid_and_type in inputs:
        total_obs_rows.append(unique_obs_hashmap[uuid_and_type[0]])

    # Build the data frame from this result
    obs_layer = pd.concat(total_obs_rows)
    return obs_layer


def generate_obs(input_csv_path: os.PathLike):
    uuids_and_types = [(r[1]['uuid'], r[1]['type']) for r in pd.read_csv(input_csv_path).iterrows()]
    __build_obs(uuids_and_types).to_csv(Path(os.environ['OUTPUT_PATH'], 'obs.csv'))


def generate(input_csv_path: os.PathLike):
    input_df = pd.read_csv(input_csv_path)

    adatas = []
    for matrix_path, grouped_df in input_df.groupby('matrix'):
        matrix = __load_matrix(matrix_path)
        obs_layer = __build_obs([(x[1]['uuid'], x[1]['type']) for x in grouped_df.iterrows()])
        adatas.append(ad.AnnData(matrix, obs_layer))

    concatenated = ad.concat(adatas)
    concatenated.write(Path(os.environ['OUTPUT_PATH'], 'output.h5ad'))

