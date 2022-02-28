import argparse
import logging
from .utils import write_json_file
import os
from .hca_lattice import HcaToLattice


def main():
    logging.basicConfig()
    logging.getLogger().setLevel(logging.INFO)

    parser = argparse.ArgumentParser(description='Convert an HCA project into a lattice project.')
    parser.add_argument('--uuid', type=str, help='HCA project UUID')
    parser.add_argument(
        '-o', '--output', type=str, help='Output directory', default=os.path.join(os.curdir, 'converted')
    )

    args = parser.parse_args()

    for sub_uuid, lattice_schema_name, entity_name, converted in HcaToLattice(args.output).convert_project(args.uuid):
        out_file = os.path.join(
            args.output,
            args.uuid,
            sub_uuid,
            lattice_schema_name,
            entity_name + '.json'
        )

        write_json_file(out_file, converted)


if __name__ == "__main__":
    main()
