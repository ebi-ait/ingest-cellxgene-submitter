# Ingest CellxGene Submitter
Tools to convert HCA projects in Ingest to a format ready for submission into [CellxGene](https://cellxgene.cziscience.com/).

## Tools
### HCA to Lattice
1. Create a virtual environment and source it
2. `pip install .`
3. `hca-to-lattice --uuid <PROJECT UUID> -o <OUTPUT DIR>`

Converted files will be put into the specified output directory in `PROJECT UUID/SUBMISSION UUID` subdirectories
