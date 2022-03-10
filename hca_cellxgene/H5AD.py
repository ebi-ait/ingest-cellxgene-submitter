import os
from pathlib import Path

import pandas as pd
import anndata as ad
import scipy.io as sio
from dotenv import load_dotenv
from pandas import SparseDtype

from hca_cellxgene.observation import IngestObservation

load_dotenv()


def __load_barcodes(barcode_file_path) -> []:
    return pd.read_csv(filepath_or_buffer=barcode_file_path, header=None)[0].array


def __load_matrix(matrix_file_path, barcodes) -> SparseDtype:
    with open(matrix_file_path) as file:
        matrix_file = sio.mmread(file)
        # Use a sparse data frame since most values are 0 and it's more efficient
        matrix = pd.DataFrame.sparse.from_spmatrix(matrix_file)
        matrix.columns = barcodes
        # TODO: Have user pass file paths or specify these directly (feature_ids, gene_names, feature_types)?
        # https://support.10xgenomics.com/single-cell-gene-expression/software/pipelines/latest/output/matrices#mat-csv
        matrix.insert(loc=0, column="feature_id", value=feature_ids)
        matrix.insert(loc=0, column="gene", value=gene_names)
        matrix.insert(loc=0, column="feature_type", value=feature_types)
        return matrix


def generate(cell_suspension_uuid, matrix_file_path, barcode_file_path, cell_type):
    # STILL UNTESTED
    # barcode_file_path will be used to generate the ID column of the obs layer
    # barcode file is a one column TSV with the same amount of rows as the transpose of the matrix
    # each barcode maps to one row of the transpose of the matrix
    # cell_type is a free text field that will be used to get the ontology of the cell_type

    # 1. get matrix file
    # 2. generate obs layer using IngestObservation(cell_suspension_uuid).to_data_frame()
    # 3. Join the matrix and obs layer
    # 5. Pass all to anndata to create h5ad https://anndata-tutorials.readthedocs.io/en/latest/getting-started.html
    # 6. Save to FS
    barcodes = __load_barcodes(barcode_file_path)
    matrix = __load_matrix(matrix_file_path, barcodes)
    obs = IngestObservation(cell_suspension_uuid, cell_type).to_data_frame()
    h5ad = ad.AnnData(matrix, obs)
    h5ad.write(Path(os.environ['OUTPUT_PATH'], cell_suspension_uuid + '.h5ad'))
