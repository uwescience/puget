"""
Functions specific to data from King County HMIS extraction.

King County data is provided in the following format:

    data/king/2011_CSV_4_6-1-15
             /2012_CSV_4_6-1-15
             /2013_CSV_4_6-2-15
             /2014_CSV_4_6-1-15/Affiliation.csv
                                Client.csv
                                Disabilities.csv
                                EmploymentEducation.csv
                                Enrollment_families.csv
                                Enrollment.csv
                                EnrollmentCoC.csv
                                Exit.csv
                                Export.csv
                                Funder.csv
                                HealthAndDV.csv
                                IncomeBenefits.csv
                                Inventory.csv
                                Organization.csv
                                Project.csv
                                ProjectCoC.csv
                                Services.csv
                                Site.csv
"""

import pandas as pd
import os.path as op
import numpy as np
import json

from data import DATA_PATH
KING_DATA = op.join(DATA_PATH, 'king')

#  Paths of csvs
FILEPATHS = {2011: '2011_CSV_4_6-1-15', 2012: '2012_CSV_4_6-1-15',
             2013: '2013_CSV_4_6-2-15', 2014: '2014_CSV_4_6-1-15'}

# these values translate to unknown data for various reasons. Treat as NANs
CATEGORICAL_UNKNOWN = [8, 9, 99]


def read_table(filename, data_dir, paths=FILEPATHS, years=None,
               columns_to_drop=None, categorical_var=None,
               categorical_unknown=CATEGORICAL_UNKNOWN,
               time_var=None, duplicate_check_columns=None):
    """
    Read in any .csv table from multiple years in the King data.

    Parameters
    ----------
    filename : string
        This should be the filename of the .csv table

    data_dir : string
        full path to general data folder (usually puget/data)

    paths : list
        list of directories inside data_dir to look for csv files in

    years : list
        list of years to include, default is to include all years

    ignore_in_dedup : list
        Generally, duplicate rows may happen when the same record is
        registered across the .csv files for each year.

    columns_to_drop : list
        A list of of columns to drop. The default is None.

    categorical_var : list
        A list of categorical (including binary) variables whose values
        8, 9, 99 should be recoded to NaNs.

    time_var : list
        A list of time (variables) in yyyy-mm-dd format that are
        reformatted into pandas timestamps. Default is None.

    Returns
    ----------
    dataframe of a csv tables from all included years
    """
    if columns_to_drop is None:
        columns_to_drop = []
    if categorical_var is None:
        categorical_var = []
    if time_var is None:
        time_var = []

    if years is None:
        years = paths.keys()
    if isinstance(years, (int, float)):
        years = [years]

    path_list = [paths[y] for y in years]

    # Start by reading the first file into a DataFrame
    df = pd.read_csv(op.join(data_dir, path_list[0], filename),
                     low_memory=False)
    df['years'] = years[0]
    # Then, for the rest of the files,
    # append to the DataFrame.
    for i in range(1, len(path_list)):
        this_df = pd.read_csv(op.join(data_dir, path_list[i], filename),
                              low_memory=False)
        this_df['years'] = years[i]
        df = df.append(this_df)

    # Drop unnecessary columns
    df = df.drop(columns_to_drop, axis=1)

    if duplicate_check_columns is None:
        print('Warning: duplicate_check_columns is None, no deduplication')
    else:
        df = df.drop_duplicates(duplicate_check_columns, keep='last',
                                inplace=False)

    # Turn values in categorical_unknown in any categorical_var into NaNs
    for col in categorical_var:
        df[col] = df[col].replace(categorical_unknown,
                                  [np.NaN, np.NaN, np.NaN])

    # Reformat yyyy-mm-dd variables to pandas timestamps
    for col in time_var:
        df[col] = pd.to_datetime(df[col], errors='coerce')
    return df


def get_metadata_dict(metadata_file):
    """Little function to read a JSON metadata file into a dict."""
    metadata_handle = open(metadata_file)
    metadata = json.loads(metadata_handle.read())
    return metadata


def get_enrollment(groups=True, filename='Enrollment.csv',
                   data_dir=KING_DATA, paths=FILEPATHS, years=None,
                   metadata_file=None, groupID_column='HouseholdID'):
    """
    Read in the Enrollment tables from King.

    Return rows with some minor clean-up that
    includes dropping unusable columns de-deplication.

    Parameters
    ----------
    groups : boolean
        If true, only return rows for groups (>1 person)

    Returns
    ----------
    dataframe with cleaned up rows of enrollments from King's Enrollment file
    """
    if metadata_file is None:
        metadata_file = op.join(DATA_PATH, 'metadata', 'king_enrollment.json')
    metadata = get_metadata_dict(metadata_file)
    _ = metadata.pop('name')
    df = read_table(filename, data_dir=data_dir, paths=paths,
                    years=years, **metadata)
    print(df.shape)
    # Now, group by HouseholdID, and only keep the groups where there are
    # more than one ProjectEntryID.
    # The new dataframe should represent families
    # (as opposed to single people).

    if groups:
        gb = df.groupby(groupID_column)

        def more_than_one(x): return (x.shape[0] > 1)
        df = gb.filter(more_than_one)

    df = df.sort_values(by=groupID_column)

    return df
