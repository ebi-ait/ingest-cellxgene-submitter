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


def create_obs_layer():
    # This CLI will not be necessary once create_h5ad works... just for testing
    parser = argparse.ArgumentParser(description='Create a CSV file for the obs layer of an h5ad file')
    parser.add_argument('--uuid', type=str, help='HCA cell suspension UUID', required=True)
    parser.add_argument(
        '-o', '--output', type=str, help='Output file', default=Path(os.environ.get('OUTPUT_PATH', 'output'), 'obs.csv')
    )
    parser.add_argument('--debug', action='store_true', default=False)

    args = parser.parse_args()

    if args.debug:
        logger.setLevel(logging.INFO)

    df = IngestObservation(args.uuid, 'test').to_data_frame()
    df.to_csv(args.output)


def create_obs_layer_from_multiple():
    parser = argparse.ArgumentParser(description='Create a CSV file for the obs layer of an h5ad file')
    parser.add_argument('--input', type=str, help='CSV of HCA cell suspension UUIDs and associated cell type on each '
                                                  'row. Expects first row to be header row of "uuid" and "cell_type',
                        required=True)
    parser.add_argument(
        '-o', '--output', type=str, help='Output file', default=Path(os.environ.get('OUTPUT_PATH', 'output'), 'obs.csv')
    )
    parser.add_argument('--debug', action='store_true', default=False)

    args = parser.parse_args()

    if args.debug:
        logger.setLevel(logging.INFO)

    uuids_and_cell_types = pd.read_csv(filepath_or_buffer=args.input)
    print(uuids_and_cell_types)
    frames = []

    # TODO Only run IngestObservation once for each unique UUID. The input csv may contain duplicate UUIDs
    # It's not very efficient right now
    for i, row in uuids_and_cell_types.iterrows():
        frames.append(IngestObservation(row['uuid'], row['cell_type']).to_data_frame())

    df = pd.concat(frames)
    df.to_csv(args.output)


def create_h5ad():
    # TODO make this take list (or TSV) of multiple matrices and their associated cell suspension_uuids, barcode,
    #  and cell types to generate one h5ad for all matrices in a given project

    parser = argparse.ArgumentParser(description='Create a CSV file for the obs layer of an h5ad file')
    parser.add_argument('--uuid', type=str, help='HCA cell suspension UUID', required=True) # should be a list of uuids?
    parser.add_argument('--matrix', type=str, help='Path to matrix file', required=True) # should be list?
    parser.add_argument('--barcode', type=str, help='Path to barcode file', required=True) # should be list?
    parser.add_argument('--type', type=str, help='Cell type', required=True) # should be list?
    parser.add_argument(
        '-o', '--output', type=str, help='Output file', default=Path(os.environ.get('OUTPUT_PATH', 'output'), 'obs.csv')
    )
    parser.add_argument('--debug', action='store_true', default=False)

    args = parser.parse_args()

    if args.debug:
        logger.setLevel(logging.INFO)

    H5AD.generate(args.uuid, args.matrix, args.barcode, args.type)
