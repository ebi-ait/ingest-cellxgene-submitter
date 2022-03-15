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
        return matrix


def __build_obs_row(cell_suspension_uuid) -> (str, DataFrame):
    return cell_suspension_uuid, IngestObservation(cell_suspension_uuid, 'test').to_data_frame()


def __build_obs(uuids: [str]) -> DataFrame:
    # Build the observation layer only for unique uuids, not for each row as each uuid may be duplicated multiple times
    # Saves network requests
    unique_uuids = set(uuids)
    with ThreadPoolExecutor() as executor:
        unique_obs_rows = executor.map(__build_obs_row, unique_uuids)

    # Create a hashmap for convenience in later lookup
    unique_obs_hashmap = {x[0]: x[1] for x in unique_obs_rows}

    # Go through each row in the original file and get the created observations
    total_obs_rows = []
    for uuid in uuids:
        total_obs_rows.append(unique_obs_hashmap[uuid])

    # Build the data frame from this result
    obs_layer = pd.concat(total_obs_rows)
    return obs_layer


def __read_input_csv(input_csv_path) -> [str]:
    return [r[1]['uuid'] for r in pd.read_csv(input_csv_path).iterrows()]


def generate_obs(input_csv_path: os.PathLike):
    rows = __read_input_csv(input_csv_path)
    __build_obs(rows).to_csv(Path(os.environ['OUTPUT_PATH'], 'obs.csv'))


def generate(input_csv_path: os.PathLike, matrix_file_path: os.PathLike):
    rows = __read_input_csv(input_csv_path)
    obs_layer = __build_obs(rows)
    matrix = __load_matrix(matrix_file_path)
    h5ad = ad.AnnData(matrix, obs_layer)
    h5ad.write(Path(os.environ['OUTPUT_PATH'], 'output.h5ad'))

