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
import datetime
import os.path as op
import numpy as np
import json
import puget.utils as pu

from puget.data import DATA_PATH
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
    _ = metadata.pop('name')
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

    filename : string
        This should be the filename of the .csv table

    data_dir : string
        full path to general data folder (usually puget/data)

    paths : list
        list of directories inside data_dir to look for csv files in

    years : list
        list of years to include, default is to include all years

    metadata_file : string
        name of json metadata file with lists of columns to use for
        deduplication, columns to drop, categorical and time-like columns

    groupID_column : string
        column to use for identifying groups within the
        enrollments (ie households)

    Returns
    ----------
    dataframe with cleaned up rows of enrollments from King's Enrollment file
    """
    if metadata_file is None:
        metadata_file = op.join(DATA_PATH, 'metadata', 'king_enrollment.json')
    metadata = get_metadata_dict(metadata_file)
    df = read_table(filename, data_dir=data_dir, paths=paths,
                    years=years, **metadata)
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


def get_exit(filename='Exit.csv',
             data_dir=KING_DATA, paths=FILEPATHS, years=None,
             metadata_file=None, df_destination_colname='Destination'):
    """
    Read in the Exit tables from King and map Destinations.

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

    metadata_file : string
        name of json metadata file with lists of columns to use for
        deduplication, columns to drop, categorical and time-like columns

    df_destination_colname : string
        column containing the numeric destination codes

    Returns
    ----------
    dataframe with rows representing exit record of a person per project
    """
    if metadata_file is None:
        metadata_file = op.join(DATA_PATH, 'metadata', 'king_exit.json')
    metadata = get_metadata_dict(metadata_file)
    df = read_table(filename, data_dir=data_dir, paths=paths,
                    years=years, **metadata)

    df_merge = pu.merge_destination(df, df_destination_colname='Destination')

    return df


def get_client(filename='Client.csv',
               data_dir=KING_DATA, paths=FILEPATHS, years=None,
               metadata_file=None, df_destination_colname='Destination'):
    """
    Read in the Exit tables from King and map Destinations.

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

    metadata_file : string
        name of json metadata file with lists of columns to use for
        deduplication, columns to drop, categorical and time-like columns

    Returns
    ----------
    dataframe with rows representing demographic information of a person
    """
    if metadata_file is None:
        metadata_file = op.join(DATA_PATH, 'metadata', 'king_client.json')
    metadata = get_metadata_dict(metadata_file)

    # Don't want to deduplicate before checking if DOB is sane because the last
    # entry is taken in deduplication but the first entry indicates how early
    # they entered the system
    duplicate_check_columns = metadata.pop('duplicate_check_columns')
    boolean_cols = metadata.pop('boolean')
    numeric_cols = metadata.pop('numeric_code')
    pid_column = metadata.pop('pid_column')

    df = read_table(filename, data_dir=data_dir, paths=paths,
                    years=years, **metadata)
    df = df.set_index(np.arange(df.shape[0]))

    bad_dob = np.logical_or(df.DOB >
                            pd.to_datetime(df.years.astype(str) +
                                           "/12/31", format='%Y/%m/%d'),
                            df.DOB < pd.to_datetime('1900/1/1',
                                                    format='%Y/%m/%d'))
    n_bad_dob = np.sum(bad_dob)

    # set any bad DOBs to NaNs. Also set to NaN if the same DOB looks bad in
    # one year but ok in the other -- ie if the DOB was in the future when it
    # was first entered it but in the past later
    gb = df.groupby(pid_column)
    for pid, group in gb:
        if np.sum(bad_dob[group.index]) > 0:
            n_entries = group.shape[0]
            if n_entries == 1:
                df.loc[group.index, 'DOB'] = pd.NaT
            else:
                if max(group.DOB) == min(group.DOB):
                    df.loc[group.index, 'DOB'] = pd.NaT
                else:
                    df.loc[group[bad_dob].index, 'DOB'] = pd.NaT

    print('Found %d entries with bad DOBs' % n_bad_dob)

    # drop years column -- this is the year associated with the csv file
    df = df.drop('years', axis=1)
    # perform deduplication that was skipped in read_table
    df = df.drop_duplicates(duplicate_check_columns, keep='last',
                            inplace=False)

    # iterate through people with more than one entry and resolve differences.
    # Set all rows to the same sensible value
    gb = df.groupby(pid_column)
    n_entries = gb.size()
    for pid, group in gb:
        if n_entries.loc[pid] > 1:
            # for differences in time columns, if the difference is less than
            # a year then take the midpoint, otherwise set to NaN
            for col in metadata['time_var']:
                if len(np.unique(group[col])) > 1:
                    is_valid = ~pd.isnull(group[col])
                    n_valid = np.sum(is_valid)
                    if n_valid == 1:
                        group[col] = group[col][is_valid]
                    elif n_valid > 1:
                        t_diff = np.max(group[col]) - np.min(group[col])
                        if t_diff < datetime.timedelta(365):
                            new_date = (np.min(group[col]) + t_diff).date()
                            group[col] = pd.datetime(new_date.year,
                                                     new_date.month,
                                                     new_date.day)
                        else:
                            group[col] = pd.NaT

            # for differences in boolean columns, if ever true then set to true
            for col in boolean_cols:
                if len(np.unique(group[col])) > 1:
                    is_valid = ~pd.isnull(group[col])
                    n_valid = np.sum(is_valid)
                    if n_valid == 1:
                        group[col] = group[col][is_valid]
                    elif n_valid > 1:
                        group[col] = np.max(group[col][is_valid])

            # for differences in numeric type columns, if there are conflicting
            # valid answers, set to NaN
            for col in numeric_cols:
                if len(np.unique(group[col])) > 1:
                    is_valid = ~pd.isnull(group[col])
                    n_valid = np.sum(is_valid)
                    if n_valid == 1:
                        group[col] = group[col][is_valid]
                    elif n_valid > 1:
                        group[col] = np.nan

            # push these changes back into the dataframe
            df.iloc[np.where(df[pid_column] == pid)[0]] = group

    # Now all rows with the same pid_column are identical, so remove them.
    df = df.drop_duplicates(pid_column, keep='last', inplace=False)

    return df
