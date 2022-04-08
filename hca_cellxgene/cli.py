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
    parser.add_argument('--uuid', help='Cell suspension UUID', type=str)
    parser.add_argument('--type', help='Cell type', type=str)
    parser.add_argument('--rows', help='Count of the number of rows in output CSV. The row will be duplicated '
                                       'this number of times', type=int, default=1)
    parser.add_argument('--debug', action='store_true', default=False)
    parser.add_argument('--csv', help="CSV of header 'uuid, type'. Each row will map to one row in the output h5ad."
                                      "Use instead of uuid, type, and rows flag")

    args = parser.parse_args()

    if args.debug:
        logger.setLevel(logging.INFO)

    if args.csv and (args.type or args.uuid):
        raise IOError("You cannot use the CSV argument as well as type and uuid.")
    if args.csv:
        H5AD.generate_obs_from_csv(args.csv)
        return

    if not args.csv and not (args.type and args.uuid):
        raise IOError("If you are not using CSV argument you must specify at least uuid and type.")
    H5AD.generate_obs(args.uuid, args.type, args.rows)


def create_h5ad():
    parser = argparse.ArgumentParser(description='Create a CSV file for the obs layer of an h5ad file')
    parser.add_argument('--input', type=str, help='CSV of HCA cell suspension UUIDs and associated matrix file, '
                                                  'barcodes file, and cell type on each row. Expects first row to be '
                                                  'header row of "uuid", "matrix", "type", "barcodes"',
                        required=True)
    parser.add_argument('--title', type=str, required=True, help="Title for the uns layer")
    parser.add_argument('--x-normalization', type=str, required=True, help="Type of normalization. Used in uns layer")
    parser.add_argument(
        '-o', '--output', type=str, help='Output file', default=Path(os.environ.get('OUTPUT_PATH', 'output'), 'obs.csv')
    )
    parser.add_argument('--debug', action='store_true', default=False)

    args = parser.parse_args()

    if args.debug:
        logger.setLevel(logging.INFO)

    H5AD.generate(args.input, args.title, args.x_normalization)


if __name__ == "__main__":
    create_h5ad()
