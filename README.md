# Ingest CellxGene Submitter
Tools to convert HCA projects in Ingest to a format ready for submission into [CellxGene](https://cellxgene.cziscience.com/).

## Steps to generate h5ad file (proposed)
1. Identify a project that is ready to submit to cellxgene
1. Identify and download the contributor generated matrices
1. Find the associated cell suspensions for each matrix
1. Run the tool for each cell suspension uuid and matrix

## Tools
### Create obs layer
1. clone this repo and cd to it
1. Create a virtual environment and source it
1. `pip install .`
1. `mkdir output`
1. `create-obs-layer --uuid <CELL SUSPENSION UUID>`

### Create obs layer from multiple cell suspension UUIDS
Same as the above but takes a CSV of cell suspension UUIDs and cell types
1. Clone this repo and cd to it
1. create a venv and source it
1. `pip install .`
1. Create a CSV of cell suspension UUIDs and cell types with a header row of `uuid,cell_type`
1. `mkdir output`
1. `create-obs-layer-from-multiple --input <PATH TO YOUR CSV>`
