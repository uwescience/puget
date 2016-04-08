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
import warnings

from puget.data import DATA_PATH
KING_DATA = op.join(DATA_PATH, 'king')

#  Paths of csvs
FILEPATHS = {2011: '2011_CSV_4_6-1-15', 2012: '2012_CSV_4_6-1-15',
             2013: '2013_CSV_4_6-2-15', 2014: '2014_CSV_4_6-1-15'}

# these values translate to unknown data for various reasons. Treat as NANs
CATEGORICAL_UNKNOWN = [8, 9, 99]

# entry/exit suffixes for columns
ENTRY_EXIT_SUFFIX = ['_entry', '_exit', '_update']

# dict of default metadata file names
METADATA_FILES = {'enrollment': 'king_enrollment.json',
                  'exit': 'king_exit.json',
                  'client': 'king_client.json',
                  'disabilities': 'king_disabilities.json',
                  'employment_education': 'king_employment_education.json',
                  'health_dv': 'king_health_dv.json',
                  'income': 'king_income.json',
                  'project': 'king_project.json'}

for k, v in METADATA_FILES.items():
    METADATA_FILES[k] = op.join(DATA_PATH, 'metadata', v)

file_path_boilerplate = (
    """
    file_spec : dict or string
        if a dict, keys should be years, values should be full path to files
        if a string, should be the filename of the .csv table and data_dir,
            paths and years parameters are required

    data_dir : string
        full path to general data folder (usually puget/data);
            not required if file_spec is a dictionary

    paths : list
        list of directories inside data_dir to look for csv files in;
            not required if file_spec is a dictionary

    years : list
        list of years to include, default is to include all years;
            not required if file_spec is a dictionary
    """)
metdata_boilerplate = (
    """
    metadata_file : string
        name of json metadata file with lists of columns to use for
        deduplication, columns to drop, categorical and time-like columns
    """)


def std_path_setup(filename, data_dir=None, paths=FILEPATHS, years=None):
    """
    Setup filenames for read_table assuming standard data directory structure.

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

    Returns
    ----------
    dict with key of years, value of filenames for all included years
    """
    if years is None:
        years = paths.keys()
    if isinstance(years, (int, float)):
        years = [years]

    path_list = [paths[y] for y in years]
    file_list = []
    for path in path_list:
        file_list.append(op.join(data_dir, path, filename))

    file_spec = dict(zip(years, file_list))
    return file_spec


def read_table(file_spec, data_dir=DATA_PATH, paths=FILEPATHS, years=None,
               columns_to_drop=None, categorical_var=None,
               categorical_unknown=CATEGORICAL_UNKNOWN,
               time_var=None, duplicate_check_columns=None, dedup=True):
    """
    Read in any .csv table from multiple years in the King data.

    Parameters
    ----------
    %s

    columns_to_drop : list
        A list of of columns to drop. The default is None.

    categorical_var : list
        A list of categorical (including binary) variables where values
        listed in categorical_unknown should be recorded as NaNs

    categorical_unknown: list
        values that should be recorded as NaNs for categorical variables
        typically: 8, 9, 99

    time_var : list
        A list of time (variables) in yyyy-mm-dd format that are
        reformatted into pandas timestamps. Default is None.

    duplicate_check_columns : list
        list of columns to conside in deduplication.
          Generally, duplicate rows may happen when the same record is
          registered across the .csv files for each year.

    dedup: boolean
        flag to turn on/off deduplication. Defaults to True

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

    if not isinstance(file_spec, dict):
        if data_dir is None:
            raise ValueError(
                'If file_spec is a string, data_dir must be passed')
        file_spec = std_path_setup(file_spec, data_dir=data_dir, paths=paths)
    else:
        if data_dir is not None or paths is not None:
            raise ValueError(
                'If file_spec is a dict, data_dir and paths cannot be passed')

    file_spec_use = file_spec.copy()

    # Start by reading the first file into a DataFrame
    year, fname = file_spec_use.popitem()
    df = pd.read_csv(fname, low_memory=False)
    df['years'] = year

    # Then, for the rest of the files, append to the DataFrame.
    for year, fname in file_spec_use.items():
        this_df = pd.read_csv(fname, low_memory=False)
        this_df['years'] = year
        df = df.append(this_df)

    # Drop unnecessary columns
    df = df.drop(columns_to_drop, axis=1)

    if dedup:
        if duplicate_check_columns is None:
            warnings.warn('dedup is True but duplicate_check_columns is ' +
                          'None, no deduplication')
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

read_table.__doc__ = read_table.__doc__ % file_path_boilerplate


def split_rows_to_columns(df, category_column, category_suffix, merge_columns):
    """
    create separate entry and exit columns for dataframes that have that
    information provided as a column giving the collection stage
    (coded as numerical values) and other columns containing the measurements
    at entry/exit

    Parameters
    ----------
    df: dataframe
        input dataframe

    category_column : string
        name of column containing the categories to be remapped to columns

    category_suffix : dict
        keys are values in category_column, values are suffixes to attach to
        the column for that category

    merge_columns: list or string
        name(s) of column(s) containing to merge on.

    Returns
    ----------
    new dataframe with response columns split into *_entry and *_exit columns
    """
    columns_to_rename = list(df.columns.values)
    if isinstance(merge_columns, list):
        for col in merge_columns:
            columns_to_rename.remove(col)
    else:
        columns_to_rename.remove(merge_columns)

    if isinstance(category_column, (list, tuple)):
        e_s = "The type column (e.g. 'CollectionStage') needs to be defined as"
        e_s += "a single string in the relevant metadata file. Cannot be a "
        e_s += "container!"
        raise TypeError(e_s)

    columns_to_rename.remove(category_column)

    # group by each type in turn
    gb = df.groupby(category_column)
    for index, tpl in enumerate(gb):
        name, group = tpl
        rename_dict = dict(zip(
            columns_to_rename,
            [s + category_suffix[name] for s in columns_to_rename]))
        this_df = group.rename(columns=rename_dict).drop(category_column,
                                                         axis=1)
        if index == 0:
            df_wide = this_df
        else:
            df_wide = pd.merge(df_wide, this_df, how='outer',
                               left_on=merge_columns, right_on=merge_columns)
    return df_wide


def read_entry_exit_table(metadata, file_spec=None, data_dir=None,
                          paths=FILEPATHS, years=None,
                          suffixes=ENTRY_EXIT_SUFFIX):
    """
    Read in tables with entry & exit values, convert entry & exit rows to
    columns

    Parameters
    ----------
    metadata : string or dict
        if dict: metadata dict
        if string: name of json metadata file
        lists of columns to use for deduplication, columns to drop,
        categorical and time-like columns
        ALSO names of columns containing collection stage and
            person_enrollment_IDs, and values indicating entry and exit
            collection stage

    %s

    Returns
    ----------
    dataframe with one row per person per enrollment -- rows containing
    separate entry & exit values are combined with different columns for
    entry & exit
    """
    if not isinstance(metadata, dict):
        metadata = get_metadata_dict(metadata)
    extra_metadata = {'collection_stage_column': None,
                      'entry_stage_val': None,
                      'exit_stage_val': None,
                      'update_stage_val': None,
                      'person_enrollment_ID': None}

    for k in extra_metadata:
        if k in metadata:
            extra_metadata[k] = metadata.pop(k)
        else:
            raise ValueError(k + ' entry must be present in metadata file')

    df = read_table(file_spec, data_dir=data_dir, paths=paths,
                    years=years, **metadata)

    # Don't use the update stage data:
    df = df[df[extra_metadata['collection_stage_column']] !=
            extra_metadata['update_stage_val']]

    df_wide = split_rows_to_columns(
            df, extra_metadata['collection_stage_column'],
            dict(zip([extra_metadata['entry_stage_val'],
                      extra_metadata['exit_stage_val']], suffixes)),
            extra_metadata['person_enrollment_ID'])

    return df_wide

read_entry_exit_table.__doc__ = read_entry_exit_table.__doc__ % (
        file_path_boilerplate)


def get_metadata_dict(metadata_file):
    """Little function to read a JSON metadata file into a dict."""
    metadata_handle = open(metadata_file)
    metadata = json.loads(metadata_handle.read())
    _ = metadata.pop('name')
    return metadata


def get_enrollment(groups=True, file_spec=None,
                   data_dir=KING_DATA, paths=FILEPATHS, years=None,
                   metadata_file=METADATA_FILES['enrollment']):
    """
    Read in the Enrollment tables from King.

    Return rows with some minor clean-up that
    includes dropping unusable columns de-deplication.

    Parameters
    ----------
    %s

    %s

    groups : boolean
        If true, only return rows for groups (>1 person)

    Returns
    ----------
    dataframe with rows representing enrollment record of a person per
        enrollment, optionally with people who are not in groups removed
    """
    if file_spec is None:
        file_spec = 'Enrollment.csv'

    metadata = get_metadata_dict(metadata_file)
    groupID_column = metadata.pop('groupID_column')
    enid_column = metadata.pop('person_enrollment_ID')
    pid_column = metadata.pop('person_ID')
    prid_column = metadata.pop('program_ID')

    df = read_table(file_spec, data_dir=data_dir, paths=paths,
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

get_enrollment.__doc__ = get_enrollment.__doc__ % (file_path_boilerplate,
                                                   metdata_boilerplate)


def get_exit(file_spec=None, data_dir=KING_DATA, paths=FILEPATHS, years=None,
             metadata_file=METADATA_FILES['exit']):
    """
    Read in the Exit tables from King and map Destinations.

    Parameters
    ----------
    %s

    %s

    Returns
    ----------
    dataframe with rows representing exit record of a person per enrollment
    """
    if file_spec is None:
        file_spec = 'Exit.csv'

    metadata = get_metadata_dict(metadata_file)
    df_destination_column = metadata.pop('destination_column')
    enid_column = metadata.pop('person_enrollment_ID')
    df = read_table(file_spec, data_dir=data_dir, paths=paths,
                    years=years, **metadata)

    df_merge = pu.merge_destination(
        df, df_destination_column=df_destination_column)

    return df_merge

get_exit.__doc__ = get_exit.__doc__ % (file_path_boilerplate,
                                       metdata_boilerplate)


def get_client(file_spec=None, data_dir=KING_DATA, paths=FILEPATHS, years=None,
               metadata_file=METADATA_FILES['client']):
    """
    Read in the Client tables from King and map Destinations.

    Parameters
    ----------
    %s

    %s

    Returns
    ----------
    dataframe with rows representing demographic information of a person
    """
    if file_spec is None:
        file_spec = 'Client.csv'

    metadata = get_metadata_dict(metadata_file)
    # Don't want to deduplicate before checking if DOB is sane because the last
    # entry is taken in deduplication but the first entry indicates how early
    # they entered the system
    duplicate_check_columns = metadata.pop('duplicate_check_columns')
    if 'boolean' in metadata:
        boolean_cols = metadata.pop('boolean')
    else:
        boolean_cols = []
        warnings.warn('boolean_cols is None')
    if 'numeric_code' in metadata:
        numeric_cols = metadata.pop('numeric_code')
    else:
        numeric_cols = []
        warnings.warn('numeric_cols is None')
    if 'person_ID' in metadata:
        pid_column = metadata.pop('person_ID')
    else:
        raise ValueError('person_ID entry must be present in metadata file')

    dob_column = metadata.pop("dob_column")

    df = read_table(file_spec, data_dir=data_dir, paths=paths,
                    years=years, dedup=False, **metadata)
    df = df.set_index(np.arange(df.shape[0]))

    bad_dob = np.logical_or(df[dob_column] >
                            pd.to_datetime(df.years.astype(str) +
                                           "/12/31", format='%Y/%m/%d'),
                            df[dob_column] < pd.to_datetime(
                                '1900/1/1', format='%Y/%m/%d'))
    n_bad_dob = np.sum(bad_dob)

    # set any bad DOBs to NaNs. Also set to NaN if the same DOB looks bad in
    # one year but ok in the other -- ie if the DOB was in the future when it
    # was first entered it but in the past later
    gb = df.groupby(pid_column)
    for pid, group in gb:
        if np.sum(bad_dob[group.index]) > 0:
            n_entries = group.shape[0]
            if n_entries == 1:
                df.loc[group.index, dob_column] = pd.NaT
            else:
                if max(group[dob_column]) == min(group[dob_column]):
                    df.loc[group.index, dob_column] = pd.NaT
                else:
                    df.loc[group.index[np.where(bad_dob[group.index])],
                           dob_column] = pd.NaT

    print('Found %d entries with bad DOBs' % n_bad_dob)

    # drop years column -- this is the year associated with the csv file
    df = df.drop('years', axis=1)
    # perform partial deduplication that was skipped in read_table,
    #  but don't deduplicate time_var, boolean or numeric columns until after
    #  resolving differences
    mid_dedup_cols = list(set(list(duplicate_check_columns) +
                              list(metadata['time_var']) +
                              list(boolean_cols) + list(numeric_cols) +
                              [pid_column]))
    df = df.drop_duplicates(mid_dedup_cols, keep='last', inplace=False)

    # iterate through people with more than one entry and resolve differences.
    # Set all rows to the same sensible value
    gb = df.groupby(pid_column)
    n_entries = gb.size()
    for pid, group in gb:
        # turn off SettingWithCopy warning for this object
        group.is_copy = False

        if n_entries.loc[pid] > 1:
            # for differences in time columns, if the difference is less than
            # a year then take the midpoint, otherwise set to NaN
            for col in metadata['time_var']:
                if len(np.unique(group[col])) > 1:
                    is_valid = ~pd.isnull(group[col])
                    n_valid = np.sum(is_valid)
                    if n_valid == 1:
                        group[col] = group[col][is_valid].values[0]
                    elif n_valid > 1:
                        t_diff = np.max(group[col]) - np.min(group[col])
                        if t_diff < datetime.timedelta(365):
                            t_diff_sec = t_diff.seconds + 86400 * t_diff.days
                            new_date = (np.min(group[col]) +
                                        datetime.timedelta(
                                            seconds=t_diff_sec / 2.)).date()
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
                        group[col] = group[col][is_valid].values[0]
                    elif n_valid > 1:
                        group[col] = np.max(group[col][is_valid])

            # for differences in numeric type columns, if there are conflicting
            # valid answers, set to NaN
            for col in numeric_cols:
                if len(np.unique(group[col])) > 1:
                    is_valid = ~pd.isnull(group[col])
                    n_valid = np.sum(is_valid)
                    if n_valid == 1:
                        group[col] = group[col][is_valid].values[0]
                    elif n_valid > 1:
                        group[col] = np.nan

            # push these changes back into the dataframe
            df.iloc[np.where(df[pid_column] == pid)[0]] = group

    # Now all rows with the same pid_column have identical time_var,
    # boolean & numeric_col values so we can perform full deduplication
    # that was skipped in read_table,
    df = df.drop_duplicates(duplicate_check_columns, keep='last',
                            inplace=False)

    return df

get_client.__doc__ = get_client.__doc__ % (file_path_boilerplate,
                                           metdata_boilerplate)


def get_disabilities(file_spec=None,  data_dir=KING_DATA, paths=FILEPATHS,
                     years=None,
                     metadata_file=METADATA_FILES['disabilities'],
                     disability_type_file=op.join(DATA_PATH, 'metadata',
                                                  'disability_type.json')):
    """
    Read in the Disabilities tables from King, convert sets of disablity type
    and response rows to columns to reduce to one row per
    primaryID (ie ProjectEntryID) with a column per disability type

    Parameters
    ----------
    %s

    %s

    disability_type_file : string
        name of json file with mapping between disability numeric codes and
        string description

    Returns
    ----------
    dataframe with rows representing presence of disability types at entry &
        exit of a person per enrollment
    """
    if file_spec is None:
        file_spec = 'Disabilities.csv'

    metadata = get_metadata_dict(metadata_file)
    extra_metadata = {'type_column': None,
                      'response_column': None}

    for k in extra_metadata:
        if k in metadata:
            extra_metadata[k] = metadata.pop(k)
        else:
            raise ValueError(k + ' entry must be present in metadata file')

    extra_metadata['person_enrollment_ID'] = metadata['person_enrollment_ID']

    stage_suffixes = ENTRY_EXIT_SUFFIX
    df_stage = read_entry_exit_table(metadata, file_spec=file_spec,
                                     data_dir=data_dir, paths=paths,
                                     years=years, suffixes=stage_suffixes)

    mapping_dict = get_metadata_dict(disability_type_file)
    # convert to integer keys
    mapping_dict = {int(k): v for k, v in mapping_dict.items()}

    type_suffixes = ['_' + s for s in mapping_dict.values()]
    merge_columns = [extra_metadata['person_enrollment_ID'],
                     extra_metadata['type_column'] + stage_suffixes[1],
                     extra_metadata['response_column'] + stage_suffixes[1]]

    df_type1 = split_rows_to_columns(df_stage, (extra_metadata['type_column'] +
                                                stage_suffixes[0]),
                                     dict(zip(list(mapping_dict.keys()),
                                          type_suffixes)), merge_columns)

    merge_columns = [extra_metadata['person_enrollment_ID']]
    for ts in type_suffixes:
        col = extra_metadata['response_column'] + stage_suffixes[0] + ts
        if col in list(df_type1.columns.values):
            merge_columns.append(col)
    df_wide = split_rows_to_columns(df_type1, (extra_metadata['type_column'] +
                                    stage_suffixes[1]),
                                    dict(zip(list(mapping_dict.keys()),
                                             type_suffixes)), merge_columns)

    response_cols = []
    new_cols = []
    for ss in stage_suffixes:
        for i, ts in enumerate(type_suffixes):
            col = extra_metadata['response_column'] + ss + ts
            if col in list(df_wide.columns.values):
                response_cols.append(col)
                new_cols.append(ts[1:] + ss)

    rename_dict = dict(zip(response_cols, new_cols))
    df_wide = df_wide.rename(columns=rename_dict)

    return df_wide

get_disabilities.__doc__ = get_disabilities.__doc__ % (file_path_boilerplate,
                                                       metdata_boilerplate)


def get_employment_education(file_spec=None, data_dir=KING_DATA,
                             paths=FILEPATHS, years=None,
                             metadata_file=METADATA_FILES['employment_education']):
    """
    Read in the EmploymentEducation tables from King.

    Parameters
    ----------
    %s

    %s

    Returns
    ----------
    dataframe with rows representing employment and education at entry & exit
              of a person per enrollment
    """
    if file_spec is None:
        file_spec = 'EmploymentEducation.csv'

    df_wide = read_entry_exit_table(metadata_file, file_spec=file_spec,
                                    data_dir=data_dir, paths=paths,
                                    years=years)

    return df_wide

get_employment_education.__doc__ = get_employment_education.__doc__ % (
    file_path_boilerplate, metdata_boilerplate)


def get_health_dv(file_spec=None, data_dir=KING_DATA, paths=FILEPATHS,
                  years=None,
                  metadata_file=METADATA_FILES['health_dv']):
    """
    Read in the HealthAndDV tables from King.

    Parameters
    ----------
    %s

    %s

    Returns
    ----------
    dataframe with rows representing employment and education at entry & exit
              of a person per enrollment
    """
    if file_spec is None:
        file_spec = 'HealthAndDV.csv'

    df_wide = read_entry_exit_table(metadata_file, file_spec=file_spec,
                                    data_dir=data_dir, paths=paths,
                                    years=years)

    return df_wide

get_health_dv.__doc__ = get_health_dv.__doc__ % (file_path_boilerplate,
                                                 metdata_boilerplate)


def get_income(file_spec=None, data_dir=KING_DATA, paths=FILEPATHS, years=None,
               metadata_file=METADATA_FILES['income']):
    """
    Read in the IncomeBenefits tables from King.

    Parameters
    ----------
    %s

    %s

    Returns
    ----------
    dataframe with rows representing income at entry & exit of a person per
        enrollment
    """
    if file_spec is None:
        file_spec = 'IncomeBenefits.csv'

    metadata = get_metadata_dict(metadata_file)
    if 'columns_to_take_max' in metadata:
        columns_to_take_max = metadata.pop('columns_to_take_max')
    else:
        raise ValueError('columns_to_take_max entry must be present in' +
                         ' metadata file')
    person_enrollment_ID = metadata['person_enrollment_ID']

    suffixes = ENTRY_EXIT_SUFFIX
    df_wide = read_entry_exit_table(metadata, file_spec=file_spec,
                                    data_dir=data_dir, paths=paths,
                                    years=years, suffixes=suffixes)

    maximize_cols = []
    for sf in suffixes:
        for col in columns_to_take_max:
            colname = col + sf
            maximize_cols.append(colname)

    non_max_cols = [x for x in df_wide.columns.values
                    if x not in maximize_cols]
    for col in non_max_cols:
        if (col != person_enrollment_ID):
            warnings.warn(col + ' column is not the person_enrollment_ID and' +
                          ' is not in maximize_cols so only the first value' +
                          ' per projectID per entry or exit will be kept')

    gb = df_wide.groupby(person_enrollment_ID)
    for index, tpl in enumerate(gb):
        name, group = tpl
        update_dict = {}
        for col in maximize_cols:
            if col in group.columns:
                update_dict[col] = [group[col].max()]
        for col in non_max_cols:
            update_dict[col] = group[col].iloc[0]
        this_df = pd.DataFrame(data=update_dict, index=[index])
        if index == 0:
            new_df = this_df
        else:
            new_df = new_df.append(this_df)

    return new_df

get_income.__doc__ = get_income.__doc__ % (file_path_boilerplate,
                                           metdata_boilerplate)


def get_project(file_spec=None, data_dir=KING_DATA, paths=FILEPATHS,
                years=None, metadata_file=METADATA_FILES['project'],
                project_type_file=op.join(DATA_PATH, 'metadata',
                                          'project_type.json')):
    """
    Read in the Exit tables from King and map Destinations.

    Parameters
    ----------
    %s

    %s

    Returns
    ----------
    dataframe with rows representing exit record of a person per enrollment
    """
    if file_spec is None:
        file_spec = 'Project.csv'

    metadata = get_metadata_dict(metadata_file)
    project_type_column = metadata.pop('project_type_column')
    projectID = metadata.pop('program_ID')
    df = read_table(file_spec, data_dir=data_dir, paths=paths,
                    years=years, **metadata)

    # get project_type dict
    mapping_dict = get_metadata_dict(project_type_file)
    # convert to integer keys
    mapping_dict = {int(k): v for k, v in mapping_dict.items()}

    map_df = pd.DataFrame(columns=['ProjectNumeric'],
                          data=list(mapping_dict.keys()))
    map_df['ProjectType'] = list(mapping_dict.values())

    df_merge = pd.merge(left=df, right=map_df, how='left',
                        left_on=project_type_column,
                        right_on='ProjectNumeric')
    df_merge = df_merge.drop(project_type_column, axis=1)

    return df_merge

get_project.__doc__ = get_project.__doc__ % (file_path_boilerplate,
                                             metdata_boilerplate)


def merge_tables(meta_files=METADATA_FILES, data_dir=KING_DATA,
                 paths=FILEPATHS, files=None, groups=True, years=None):
    """ Run all functions that clean up King tables separately, and merge them
        all into the enrollment table, where each row represents the project
        enrollment of an individual.

        Parameters
        ----------
        meta_files: dict
            dictionary giving names of metadata files for each table type
            If any table type is missing it is defaulted using METADATA_FILES

        files: dict
            dictionary giving short data file names for each table type.
                (these must be combined with data_dir, paths and years to get
                the full file names)
            If any table type is missing the file name is defaulted in the
            respective get_* functions

        data_dir : string
            full path to general data folder (usually puget/data)

        paths : list
            list of directories inside data_dir to look for csv files in

        years : list
            list of years to include, default is to include all years

        Returns
        ----------
        dataframe with rows representing the record of a person per
        project enrollment
    """
    if not isinstance(files, dict):
        files = {}

    # Get enrollment data
    enroll = get_enrollment(file_spec=files.get('enrollment', None),
                            metadata_file=meta_files.get('enrollment', None),
                            groups=groups, years=years, data_dir=data_dir,
                            paths=paths)

    enrollment_metadata = get_metadata_dict(meta_files.get('enrollment',
                                            METADATA_FILES['enrollment']))
    enrollment_enid_column = enrollment_metadata['person_enrollment_ID']
    enrollment_pid_column = enrollment_metadata['person_ID']
    enrollment_prid_column = enrollment_metadata['program_ID']
    # print(enroll)

    # Merge exit in
    exit_table = get_exit(file_spec=files.get('exit', None),
                          metadata_file=meta_files.get('exit', None),
                          years=years, data_dir=data_dir, paths=paths)
    exit_metadata = get_metadata_dict(meta_files.get('exit',
                                      METADATA_FILES['exit']))
    exit_ppid_column = exit_metadata['person_enrollment_ID']

    enroll_merge = pd.merge(left=enroll, right=exit_table, how='left',
                            left_on=enrollment_enid_column,
                            right_on=exit_ppid_column)

    if enrollment_enid_column != exit_ppid_column and \
            exit_ppid_column in enroll_merge.columns:
        enroll_merge = enroll_merge.drop(exit_ppid_column, axis=1)

    # Merge client in
    client = get_client(file_spec=files.get('client', None),
                        metadata_file=meta_files.get('client', None),
                        years=years, data_dir=data_dir, paths=paths)
    client_metadata = get_metadata_dict(meta_files.get('client',
                                        METADATA_FILES['client']))
    client_pid_column = client_metadata['person_ID']

    enroll_merge = pd.merge(left=enroll_merge, right=client, how='left',
                            left_on=enrollment_pid_column,
                            right_on=client_pid_column)

    if enrollment_pid_column != client_pid_column and \
            client_pid_column in enroll_merge.columns:
        enroll_merge = enroll_merge.drop(client_pid_column, axis=1)

    # Merge disabilities in
    disabilities = get_disabilities(file_spec=files.get('disabilities', None),
                                    metadata_file=meta_files.get('disabilities', None),
                                    years=years, data_dir=data_dir,
                                    paths=paths)
    disabilities_metadata = get_metadata_dict(meta_files.get('disabilities',
                                              METADATA_FILES['disabilities']))
    disabilities_ppid_column = disabilities_metadata['person_enrollment_ID']
    enroll_merge = enroll_merge.merge(disabilities, how='left',
                                      left_on=enrollment_enid_column,
                                      right_on=disabilities_ppid_column)

    if enrollment_enid_column != disabilities_ppid_column and \
            disabilities_ppid_column in enroll_merge.columns:
        enroll_merge = enroll_merge.drop(disabilities_ppid_column, axis=1)

    # Merge employment_education in
    emp_edu = get_employment_education(file_spec=files.get('employment_education', None),
                                       metadata_file=meta_files.get('employment_education', None),
                                       years=years, data_dir=data_dir,
                                       paths=paths)
    emp_edu_metadata = get_metadata_dict(meta_files.get('employment_education',
                                         METADATA_FILES['employment_education']))
    emp_edu_ppid_column = emp_edu_metadata['person_enrollment_ID']
    enroll_merge = enroll_merge.merge(emp_edu, how='left',
                                      left_on=enrollment_enid_column,
                                      right_on=emp_edu_ppid_column)

    if enrollment_enid_column != emp_edu_ppid_column and \
            emp_edu_ppid_column in enroll_merge.columns:
        enroll_merge = enroll_merge.drop(emp_edu_ppid_column, axis=1)

    # Merge health in
    health_dv = get_health_dv(file_spec=files.get('health_dv', None),
                              metadata_file=meta_files.get('health_dv', None),
                              years=years, data_dir=data_dir, paths=paths)
    health_dv_metadata = get_metadata_dict(meta_files.get('health_dv',
                                           METADATA_FILES['health_dv']))
    health_dv_ppid_column = health_dv_metadata['person_enrollment_ID']
    enroll_merge = enroll_merge.merge(health_dv, how='left',
                                      left_on=enrollment_enid_column,
                                      right_on=health_dv_ppid_column)

    if enrollment_enid_column != health_dv_ppid_column and \
            health_dv_ppid_column in enroll_merge.columns:
        enroll_merge = enroll_merge.drop(health_dv_ppid_column, axis=1)

    # Merge income in
    income = get_income(file_spec=files.get('income', None),
                        metadata_file=meta_files.get('income', None),
                        years=years, data_dir=data_dir, paths=paths)
    income_metadata = get_metadata_dict(meta_files.get('income',
                                        METADATA_FILES['income']))
    income_ppid_column = income_metadata['person_enrollment_ID']
    enroll_merge = enroll_merge.merge(income, how='left',
                                      left_on=enrollment_enid_column,
                                      right_on=income_ppid_column)

    if enrollment_enid_column != income_ppid_column and \
            income_ppid_column in enroll_merge.columns:
        enroll_merge = enroll_merge.drop(income_ppid_column, axis=1)

    # Merge project in
    project = get_project(file_spec=files.get('project', None),
                          metadata_file=meta_files.get('project', None),
                          years=years, data_dir=data_dir, paths=paths)
    project_metadata = get_metadata_dict(meta_files.get('project',
                                         METADATA_FILES['project']))
    project_prid_column = project_metadata['program_ID']
    enroll_merge = enroll_merge.merge(project, how='left',
                                      left_on=enrollment_prid_column,
                                      right_on=project_prid_column)

    if enrollment_prid_column != project_prid_column and \
            project_prid_column in enroll_merge.columns:
        enroll_merge = enroll_merge.drop(project_prid_column, axis=1)

    return enroll_merge
