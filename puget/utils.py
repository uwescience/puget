import pandas as pd
import os.path as op
import numpy as np
from puget.data import DATA_PATH

METADATA = op.join(DATA_PATH, 'metadata')


def merge_destination(df, df_destination_colname='destination_value',
                      destination_map_fname='destination_mappings.csv',
                      directory=METADATA):
    """
    Merge a categorization of destination outcomes into a dataframe using
    the column of numeric destination outcomes as the merge-by variable.

    Parameters
    -----------
    df: a dataframe with a column that contains the numeric destination
        outcomes

    df_destination_colname: a string - the name of the column that contains the
        numeric destination outcomes

    destination_map_fname: string (optional). The filename containing the
        categorization of destination outcomes. The default is
        destination_mappings.csv in the metadata directory

    directory: string (optional). The directory containing the mapping file.

    Returns
    -------
    output_df: A pandas dataframe containing the original dataframe, plus 4 new
        columns with the mappings for destination. The new columns are :
        DestinationDescription:  Text description of destination
        DestinationGroup : it is more aggregated than DestinationDescription,
            but less than DestinationSuccess
        DestinationSuccess : a binary : two values are  'Other Exit',
            'Successful Exit' (or 'NaN')
        success_and_subsidy: a column with three possible values -- successful
            with & without subsidy, unsuccessful
        Subsidy
    """
    # Import the csv file into pandas:
    mapping_table = pd.read_csv(op.join(directory, destination_map_fname))
    mapping_table = mapping_table[mapping_table.Standard == "New Standards"]
    # Recode Subsidy column to boolean
    mapping_table['Subsidy'] = mapping_table['Subsidy'].map({'Yes': True,
                                                             'No': False})
    # Drop columns we don't need
    mapping_table = mapping_table.drop(['Standard'], axis=1)

    # Merge the Destination mapping with the df
    # based on the last_destination string
    output_df = pd.merge(left=df, right=mapping_table, how='left',
                         left_on=df_destination_colname,
                         right_on='DestinationNumeric')

    output_df = output_df.drop(df_destination_colname, axis=1)

    return output_df


def update_progress(progress):
    """Progress bar in the console.
    Inspired by
    http://stackoverflow.com/questions/3173320/text-progress-bar-in-the-console
    Parameters
    -----------
    progress : a value (float or int) between 0 and 100 indicating
               percentage progress
    """
    print('\r[%-10s] %0.2f%%' % ('#' * int(progress/10), progress))
