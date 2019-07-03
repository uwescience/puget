# puget

Tools for munging data from Puget Sound Region tri-county HMIS.

## Modules:
- utils
- preprocess
- cluster
- recordlinkage
- tests

## Dependencies:

- pandas
- numpy
- scipy
- networkx
- recordlinkage
- matplotlib
- pytest (for testing)
- sphinx (for docs)


## Steps:
   raw data => 1 row per individual per enrollment (each county)
       `raw2individual_enrollments`
   => 1 row per family per enrollment (optional, general - belongs in `???`) -
       `individuals2families`
   => 1 row per family/individual per episode (general - belongs in `???`) -
       `enrollments2episodes`
   =>
