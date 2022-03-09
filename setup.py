import os

from setuptools import setup, find_packages

base_dir = os.path.dirname(__file__)
install_requires = [line.rstrip() for line in open(os.path.join(base_dir, 'requirements.txt'))]

setup(
    name='ingest-cellxgene-submitter',
    version='0.0.1',
    packages=find_packages(),
    url='',
    license='MIT',
    author='Jacob Windsor',
    author_email='jcbwndsr@ebi.ac.uk',
    description='Convert project from HCA to cellxgene format',
    install_requires=[
        'hca-ingest @ git+https://github.com/ebi-ait/ingest-client.git@289a333#egg=hca_ingest',
        install_requires,
    ],
    entry_points={
        'console_scripts': [
            'create-obs-layer=hca_cellxgene.cli:create_obs_layer',
            'create-obs-layer-multiple=hca_cellxgene.cli:create_obs_layer_from_multiple',
            'create-h5ad=hca_cellxgene.cli:create_h5ad'
        ],
    },
    include_package_data=True
)
