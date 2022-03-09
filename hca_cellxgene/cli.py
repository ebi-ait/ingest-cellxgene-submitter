import argparse
import logging
import os
from pathlib import Path

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

    df = IngestObservation(args.uuid).to_data_frame()
    df.to_csv(args.output)


def create_h5ad():
    parser = argparse.ArgumentParser(description='Create a CSV file for the obs layer of an h5ad file')
    parser.add_argument('--uuid', type=str, help='HCA cell suspension UUID', required=True)
    parser.add_argument('--matrix', type=str, help='Path to matrix file', required=True)
    parser.add_argument(
        '-o', '--output', type=str, help='Output file', default=Path(os.environ.get('OUTPUT_PATH', 'output'), 'obs.csv')
    )
    parser.add_argument('--debug', action='store_true', default=False)

    args = parser.parse_args()

    if args.debug:
        logger.setLevel(logging.INFO)

    H5AD.generate(args.uuid, args.matrix)
