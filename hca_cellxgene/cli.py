import argparse
import logging
import os
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv

from hca_cellxgene import H5AD
from hca_cellxgene.observation import IngestObservation

logging.basicConfig()
logger = logging.getLogger()

load_dotenv()


def create_obs():
    parser = argparse.ArgumentParser(description='Create a CSV file for the obs layer of an h5ad file')
    parser.add_argument('--input', type=str, help='CSV of HCA cell suspension UUIDs and associated cell type on each '
                                                  'row. Expects first row to be header row of "index, "uuid", "type"',
                        required=True)
    parser.add_argument('--debug', action='store_true', default=False)

    args = parser.parse_args()

    if args.debug:
        logger.setLevel(logging.INFO)

    H5AD.generate_obs(args.input)


def create_h5ad():
    # TODO make this take list (or TSV) of multiple matrices and their associated cell suspension_uuids, barcode,
    #  and cell types to generate one h5ad for all matrices in a given project

    parser = argparse.ArgumentParser(description='Create a CSV file for the obs layer of an h5ad file')
    parser.add_argument('--input', type=str, help='CSV of HCA cell suspension UUIDs and associated cell type on each '
                                                  'row. Expects first row to be header row of '
                                                  '"index", "uuid", "matrix", "type"',
                        required=True)
    parser.add_argument(
        '-o', '--output', type=str, help='Output file', default=Path(os.environ.get('OUTPUT_PATH', 'output'), 'obs.csv')
    )
    parser.add_argument('--debug', action='store_true', default=False)

    args = parser.parse_args()

    if args.debug:
        logger.setLevel(logging.INFO)

    H5AD.generate(args.input)
