import pandas as pd
import os.path as op
import numpy as np
import json

from data import DATA_PATH
KING_DATA = op.join(DATA_PATH, 'king')

#Paths of csvs
FILEPATHS = {2011:'2011_CSV_4_6-1-15', 2012:'2012_CSV_4_6-1-15',
             2013:'2013_CSV_4_6-2-15',2014:'2014_CSV_4_6-1-15'}

#columns to not consider when checking for duplicate rows.
IGNORE_IN_DEDUP=['DateCreated', 'DateUpdated', 'UserID', 'DateDeleted',
                 'ExportID','CSV_directory']

# these values translate to unknown data for various reasons. Treat as NANs
CATEGORICAL_UNKNOWN = [8, 9, 99]

def read_table(filename, data_dir=KING_DATA, paths=FILEPATHS, years=None,
                   ignore_in_dedup=IGNORE_IN_DEDUP,
                   columns_to_drop=None, categorical_var=None,
                   categorical_unknown=CATEGORICAL_UNKNOWN,
                   time_var=None):
    ''' Reads in any .csv table from multiple years in the King data.

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
    '''
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

    # Drop duplicate rows, ignoring the columns in ignore_in_dedup.
    duplicate_check_columns = []
    for col in df.columns:
        if col not in ignore_in_dedup:
            duplicate_check_columns.append(col)
    df = df.drop_duplicates(duplicate_check_columns, take_last=True,
                            inplace=False)

    # Drop unnecessary columns
    df = df.drop(columns_to_drop, axis=1)

    # Turn values in categorical_unknown in any categorical_var into NaNs
    for col in categorical_var:
        df[col] = df[col].replace(categorical_unknown, [np.NaN, np.NaN, np.NaN])

    # Reformat yyyy-mm-dd variables to pandas timestamps
    for col in time_var:
        df[col] = pd.to_datetime(df[col], coerce=True)
    return df


def get_enrollment(dedup_early=False):
    ''' Reads in the Enrollment tables from King, and returns only rows that
        are families, with some minor clean-up that includes dropping unusable
        columns, and second de-deplication.

        Parameters
        ----------
        None

        Returns
        ----------
        dataframe with only rows of enrollments of multiple people from King's
        Enrollment
    '''
    metadata_file = op.join(DATA_PATH, 'metadata', 'king_enrollment.json')
    metadata_handle = open(metadata_file)
    metadata = json.loads(metadata_handle.read())
    _ = metadata.pop('name')
    if dedup_early:
        metadata['ignore_in_dedup'] = IGNORE_IN_DEDUP + ['DisablingCondition']
        df = read_table('Enrollment.csv', data_dir=KING_DATA, paths=FILEPATHS,
                    years=None, **metadata)
        print(df.shape)
        # Now, group by HouseholdID, and only keep the groups where there are
        # more than one ProjectEntryID.
        # The new dataframe should represent families
        # (as opposed to single people).
        gb = df.groupby('HouseholdID')
        more_than_one = lambda x: (len(x['ProjectEntryID']) > 1)
        families = gb.filter(more_than_one)
        print(families.shape)
        families = families.sort(columns='DisablingCondition')
        print(families.shape)
    else:
        df = read_table('Enrollment.csv', data_dir=KING_DATA, paths=FILEPATHS,
                    years=None, **metadata)
        print(df.shape)
        # Now, group by HouseholdID, and only keep the groups where there are
        # more than one ProjectEntryID.
        # The new dataframe should represent families (as opposed to single people).
        gb = df.groupby('HouseholdID')
        more_than_one = lambda x: (len(x['ProjectEntryID']) > 1)
        families = gb.filter(more_than_one)
        print(families.shape)
        # After a general de-duplication in get_king_table, there are still
        # duplicate rows of the same ProjectEntryID
        # due to minor data collection discrepancies in the column
        # Disabling Condition.
        columns_to_exclude=['DisablingCondition']
        duplicate_check_columns = []
        for col in families.columns:
            if col not in columns_to_exclude:
                duplicate_check_columns.append(col)
        families = families.sort(columns='DisablingCondition')
        print(families.shape)
        families = families.drop_duplicates(duplicate_check_columns,
                   take_last=False, inplace=False)
        print(families.shape)
    return families
