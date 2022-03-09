import os
from pathlib import Path

import pandas as pd
import anndata as ad
import scipy.io as sio
from dotenv import load_dotenv
from pandas import SparseDtype

from hca_cellxgene.observation import IngestObservation

load_dotenv()


def __load_matrix(matrix_file_path) -> SparseDtype:
    with open(matrix_file_path) as f:
        matrix = sio.mmread(f)
        # Use a sparse data frame since most values are 0 and it's more efficient
        return pd.DataFrame.sparse.from_spmatrix(matrix)


def generate(cell_suspension_uuid, matrix_file_path, barcode_file_path, cell_type):
    # UNTESTED
    # barcode_file_path will be used to generate the ID column of the obs layer
    # barcode file is a one column TSV with the same amount of rows as the transpose of the matrix
    # each barcode maps to one row of the transpose of the matrix
    # cell_type is a free text field that will be used to get the ontology of the cell_type

    # 1. get matrix file
    # 2. generate obs layer using IngestObservation(cell_suspension_uuid).to_data_frame()
    # 3. Join the matrix and obs layer
    # 5. Pass all to anndata to create h5ad https://anndata-tutorials.readthedocs.io/en/latest/getting-started.html
    # 6. Save to FS
    matrix = __load_matrix(matrix_file_path)
    obs = IngestObservation(cell_suspension_uuid).to_data_frame()
    h5ad = ad.AnnData(matrix, obs)
    h5ad.write(Path(os.environ['OUTPUT_PATH'], cell_suspension_uuid + '.h5ad'))
