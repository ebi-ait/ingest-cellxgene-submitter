# Ingest CellxGene Submitter
Tools to convert HCA projects in Ingest to a format ready for submission into [CellxGene](https://cellxgene.cziscience.com/).

## Installation
1. clone this repo and cd to it
1. Create a virtual environment and source it
1. `pip install .`
1. `mkdir output`

## Tools
### Create H5AD
1. Identify a project that is ready to submit to cellxgene
1. Identify and download the contributor generated matrices
1. Find the associated cell suspensions for each matrix
1. Create a CSV file with the header row `"uuid", "matrix", "type", "barcodes"`
    1. Each row should map to one matrix file
    1. The UUID should be the cell suspension UUID in ingest for each matrix
    1. The type should be the cell type ontology for the cell suspension
    1. Barcodes and matrix should be paths relative to the CWD to the barcode and matrix files
1. Run `create-h5ad --input <PATH TO CSV>`
    1. You can run it with the `--debug` flag if desired
1. It will output a file to `output/` that is an H5AD for all the matrices specified in the input CSV

### Create obs layer
This tool is useful if you already have an H5AD file and want to create one that is up to the cellxgene spec

1. `create-obs --uuid <cell suspension uuid> --type <cell type> --rows <number of rows`
    1. Rows should correspond to the number of rows in the X layer of the h5ad


