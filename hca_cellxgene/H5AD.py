import os
from functools import partial
from pathlib import Path

import pandas as pd
import anndata as ad
import scipy.io as sio
from dotenv import load_dotenv
from pandas import SparseDtype, DataFrame
from multiprocessing import Pool, Manager

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


def __build_obs_row(cache: dict, row: (str, str)) -> DataFrame:
    index, cell_suspension_uuid = row
    if cell_suspension_uuid not in cache:
        cache[cell_suspension_uuid] = IngestObservation(cell_suspension_uuid, 'test')
    return cache[cell_suspension_uuid].to_data_frame()


def __build_obs(input_csv_path) -> DataFrame:
    rows = ((r[1]['index'], r[1]['uuid']) for r in pd.read_csv(input_csv_path).iterrows())

    with Manager() as m:
        observation_cache = m.dict()

        with Pool() as p:
            obs_layer_rows = p.map(partial(__build_obs_row, observation_cache), rows)
            obs_layer = pd.concat(obs_layer_rows)

    return obs_layer


def generate_obs(input_csv_path):
    __build_obs(input_csv_path).to_csv(Path(os.environ['OUTPUT_PATH'], 'obs.csv'))


def generate(input_csv_path, matrix_file_path):
    obs_layer = __build_obs(input_csv_path)
    matrix = __load_matrix(matrix_file_path)
    h5ad = ad.AnnData(matrix, obs_layer)
    h5ad.write(Path(os.environ['OUTPUT_PATH'], 'output.h5ad'))

