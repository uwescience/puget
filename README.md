# puget

Tools for munging data from Puget Sound Region tri-county HMIS.

## Modules:
- utils
- king
- pierce
- snohomish
- tests

## Dependencies:

pandas
numpy
matplotlib
nose (for testing)
sphinx (for docs)


## Steps:
   raw data => 1 row per individual per enrollment (each county)
       `raw2individual_enrollments`
   => 1 row per family per enrollment (optional, general - belongs in `???`) -
       `individuals2families`
   => 1 row per family/individual per episode (general - belongs in `???`) -
       `enrollments2episodes`
   =>
