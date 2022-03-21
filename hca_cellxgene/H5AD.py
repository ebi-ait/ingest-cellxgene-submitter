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


def __load_barcodes(barcode_file_path) -> DataFrame:
    return pd.read_csv(filepath_or_buffer=barcode_file_path, header=None)


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


def generate_obs(uuid: str, cell_type: str, rows: int):
    if rows < 1:
        raise IndexError("Rows cannot be less than 1")
    obs = __build_obs_row((uuid, cell_type))
    if rows > 1:
        obs = pd.concat([obs]*rows, ignore_index=True)
    obs.to_csv(Path(os.environ['OUTPUT_PATH'], 'obs.csv'))


def generate(input_csv_path: os.PathLike):
    input_df = pd.read_csv(input_csv_path)

    # Build the observation layers for each cell suspension UUID in parallel to speed things up
    with ThreadPoolExecutor() as executor:
        unique_obs_rows = executor.map(__build_obs_row, [(x[1]['uuid'], x[1]['type']) for x in input_df.iterrows()])
        obs_map = {x[0]: x[1] for x in unique_obs_rows}

    adatas = []
    for _, row in input_df.iterrows():
        logging.info(f'Generating h5ad file for {row["uuid"]}')
        barcodes = __load_barcodes(row['barcodes'])
        obs_row_for_uuid = obs_map[row['uuid']]
        obs_layer = pd.concat([obs_row_for_uuid]*len(barcodes.index), ignore_index=True)
        matrix = __load_matrix(row['matrix'])
        adatas.append(ad.AnnData(matrix, obs_layer))

    logging.info("Concatenating all h5ads into one h5ad")
    concatenated = ad.concat(adatas)
    output_path = Path(os.environ['OUTPUT_PATH'], 'output.h5ad')
    concatenated.write(output_path)
    logging.info(f"Finished generating h5ad output and written to {output_path}.")

