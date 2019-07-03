[![CircleCI](https://circleci.com/gh/uwescience/puget.svg?style=svg)](https://circleci.com/gh/uwescience/puget)
[![codecov](https://codecov.io/gh/uwescience/puget/branch/master/graph/badge.svg)](https://codecov.io/gh/uwescience/puget)

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
