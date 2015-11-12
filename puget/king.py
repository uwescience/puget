import pandas as pd
import os.path as op
import numpy as np

#Paths of csvs
FILEPATHS = {2011:'2011_CSV_4_6-1-15', 2012:'2012_CSV_4_6-1-15',
             2013:'2013_CSV_4_6-2-15',2014:'2014_CSV_4_6-1-15'}

#columns to not consider when checking for duplicate rows.
IGNORE_IN_DEDUP=['DateCreated', 'DateUpdated', 'UserID', 'DateDeleted',
                 'ExportID','CSV_directory']

# these values translate to unknown data for various reasons. Treat as NANs
CATEGORICAL_UNKNOWN = [8, 9, 99]

def read_table(filename, data_dir, paths=FILEPATHS, years=None,
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
