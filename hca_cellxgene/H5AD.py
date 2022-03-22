import logging
import os
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path

import anndata as ad
import pandas as pd
import scipy.io as sio
from dotenv import load_dotenv
from pandas import SparseDtype, DataFrame

from hca_cellxgene.observation import IngestObservation

load_dotenv()


def __load_barcodes(barcode_file_path) -> DataFrame:
    return pd.read_csv(filepath_or_buffer=barcode_file_path, header=None)


def __load_matrix(matrix_file_path) -> SparseDtype:
    matrix_file = sio.mmread(matrix_file_path)
    # Use a sparse data frame since most values are 0 and it's more efficient
    matrix = pd.DataFrame.sparse.from_spmatrix(matrix_file)
    return matrix.transpose()


def __build_obs_row(cell_suspension_uuid: str, cell_type: str) -> (str, DataFrame):
    logging.info(f'building obs row for {cell_suspension_uuid}')
    return cell_suspension_uuid, IngestObservation(cell_suspension_uuid, cell_type).to_data_frame()


def generate_obs(uuid: str, cell_type: str, rows: int):
    if rows < 1:
        raise IndexError("Rows cannot be less than 1")
    obs = __build_obs_row(uuid, cell_type)[1]
    if rows > 1:
        obs = pd.concat([obs]*rows, ignore_index=True)
    obs.to_csv(Path(os.environ['OUTPUT_PATH'], 'obs.csv'))


def __build_h5ad(uuid: str, barcodes: str, matrix: str, obs: DataFrame) -> ad.AnnData:
    logging.info(f'Generating h5ad file for {uuid}')
    barcodes = __load_barcodes(barcodes)
    obs_layer = pd.concat([obs] * len(barcodes.index), ignore_index=True)
    matrix = __load_matrix(matrix)
    return ad.AnnData(matrix, obs_layer)


def generate(input_csv_path: os.PathLike, title: str, x_normalization: str):
    input_df = pd.read_csv(input_csv_path)

    # Build the observation layers for each cell suspension UUID in parallel to speed things up
    # Using ThreadPool as this is an IO bound task
    with ThreadPoolExecutor() as executor:
        unique_obs_rows = executor.map(__build_obs_row, input_df['uuid'], input_df['type'])
        obs_map = {x[0]: x[1] for x in unique_obs_rows}

    # Using Processes as this is a CPU bound task
    with ProcessPoolExecutor() as executor:
        obs_layers = (obs_map[x] for x in input_df['uuid'])
        adatas = executor.map(__build_h5ad, input_df['uuid'], input_df['barcodes'], input_df['matrix'], obs_layers)

    logging.info("Concatenating all h5ads into one h5ad")
    concatenated = ad.concat(adatas)
    concatenated.uns = {
        "schema_version": os.environ.get('UNS_SCHEMA_VERSION'),
        "title": title,
        "X_normalization": x_normalization,
    }
    output_path = Path(os.environ['OUTPUT_PATH'], 'output.h5ad')
    concatenated.write(output_path)
    logging.info(f"Finished generating h5ad output and written to {output_path}.")

