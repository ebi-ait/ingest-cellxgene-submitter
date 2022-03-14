import os
from pathlib import Path

import pandas as pd
import anndata as ad
import scipy.io as sio
from dotenv import load_dotenv
from pandas import SparseDtype, DataFrame
from multiprocessing import Pool

from hca_cellxgene.observation import IngestObservation

load_dotenv()

OBSERVATION_CACHE = {}


def __load_barcodes(barcode_file_path) -> []:
    return pd.read_csv(filepath_or_buffer=barcode_file_path, header=None)[0].array


def __load_matrix(matrix_file_path) -> SparseDtype:
    with open(matrix_file_path) as file:
        matrix_file = sio.mmread(file)
        # Use a sparse data frame since most values are 0 and it's more efficient
        matrix = pd.DataFrame.sparse.from_spmatrix(matrix_file)
        return matrix


def __build_obs_row(row: (str, str)) -> DataFrame:
    index, cell_suspension_uuid = row
    if cell_suspension_uuid not in OBSERVATION_CACHE:
        OBSERVATION_CACHE[cell_suspension_uuid] = IngestObservation(cell_suspension_uuid, 'test')
    return OBSERVATION_CACHE[cell_suspension_uuid].to_data_frame()


def generate(input_csv_path, matrix_file_path):
    rows = ((r[1]['index'], r[1]['uuid']) for r in pd.read_csv(input_csv_path).iterrows())

    with Pool() as p:
        obs_layer_rows = p.map(__build_obs_row, rows)
        obs_layer = pd.concat(obs_layer_rows)
        matrix = __load_matrix(matrix_file_path)
        h5ad = ad.AnnData(matrix, obs_layer)
        h5ad.write(Path(os.environ['OUTPUT_PATH'], 'output.h5ad'))

