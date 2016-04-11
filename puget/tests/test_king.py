"""Tests for functions in king.py."""
import puget.king as pk
import puget
import os
import os.path as op
import pandas as pd
import pandas.util.testing as pdt
import numpy as np
import tempfile
import json
from nose.tools import assert_equal, assert_raises


def test_std_path_setup():
    filename = 'test'
    data_dir = 'data'
    file_paths = {2011: 'test_2011', 2012: 'test_2012', 2013: 'test_2013',
                  2014: 'test_2014'}

    # test with one year
    years = 2012

    file_spec = pk.std_path_setup(filename, data_dir=data_dir,
                                  paths=file_paths, years=years)
    test_dict = {2012: op.join(data_dir, file_paths[2012], filename)}

    assert_equal(file_spec, test_dict)

    # test with limited years
    years = [2012, 2013]

    file_spec = pk.std_path_setup(filename, data_dir=data_dir,
                                  paths=file_paths, years=years)
    test_dict = {2012: op.join(data_dir, file_paths[2012], filename),
                 2013: op.join(data_dir, file_paths[2013], filename)}

    assert_equal(file_spec, test_dict)

    # test with all years
    file_spec = pk.std_path_setup(filename, data_dir=data_dir,
                                  paths=file_paths)
    test_dict = {2011: op.join(data_dir, file_paths[2011], filename),
                 2012: op.join(data_dir, file_paths[2012], filename),
                 2013: op.join(data_dir, file_paths[2013], filename),
                 2014: op.join(data_dir, file_paths[2014], filename)}

    assert_equal(file_spec, test_dict)


def test_read_table():
    """Test read_table function."""
    # create temporary csv file
    temp_csv_file = tempfile.NamedTemporaryFile(mode='w')
    df = pd.DataFrame({'id': [1, 1, 2, 2],
                       'time1': ['2001-01-13', '2004-05-21', '2003-06-10',
                                 '2003-06-10'], 'drop1': [2, 3, 4, 5],
                       'ig_dedup1': [5, 6, 7, 8], 'categ1': [0, 8, 0, 0]})
    df.to_csv(temp_csv_file, index=False)
    temp_csv_file.seek(0)

    file_spec = {2011: temp_csv_file.name}
    df = pk.read_table(file_spec, data_dir=None, paths=None,
                       columns_to_drop=['drop1'], categorical_var=['categ1'],
                       time_var=['time1'],
                       duplicate_check_columns=['id', 'time1', 'categ1'])

    df_test = pd.DataFrame({'id': [1, 1, 2],
                            'time1':
                            pd.to_datetime(['2001-01-13', '2004-05-21',
                                            '2003-06-10'], errors='coerce'),
                            'ig_dedup1': [5, 6, 8], 'categ1': [0, np.nan, 0],
                            'years': [2011, 2011, 2011]})
    # Have to change the index to match the one we de-duplicated
    df_test.index = pd.Int64Index([0, 1, 3])
    pdt.assert_frame_equal(df, df_test)

    # test passing a string filename with data_dir and path
    path, fname = op.split(temp_csv_file.name)
    path0, path1 = op.split(path)
    path_dict = {2011: path1}
    df = pk.read_table(fname, data_dir=path0, paths=path_dict,
                       columns_to_drop=['drop1'], categorical_var=['categ1'],
                       time_var=['time1'],
                       duplicate_check_columns=['id', 'time1', 'categ1'])

    temp_csv_file.close()

    # test error checking
    assert_raises(ValueError, pk.read_table, file_spec, data_dir=pk.KING_DATA)

    # test error checking
    assert_raises(ValueError, pk.read_table, 'test', data_dir=None, paths=None)


def test_read_entry_exit():
    temp_csv_file = tempfile.NamedTemporaryFile(mode='w')
    df_init = pd.DataFrame({'id': [11, 11, 12],
                            'stage': [0, 1, 0], 'value': [0, 1, 0]})
    df_init.to_csv(temp_csv_file, index=False)
    temp_csv_file.seek(0)

    temp_meta_file = tempfile.NamedTemporaryFile(mode='w')
    metadata = {'name': 'test',
                'duplicate_check_columns': ['id', 'stage', 'value'],
                'columns_to_drop': ['years'],
                'categorical_var': ['value'],
                'collection_stage_column': 'stage', 'entry_stage_val': 0,
                'exit_stage_val': 1, 'update_stage_val': 2,
                'person_enrollment_ID': 'id'}

    metadata_json = json.dumps(metadata)
    temp_meta_file.file.write(metadata_json)
    temp_meta_file.seek(0)

    file_spec = {2011: temp_csv_file.name}

    df = pk.read_entry_exit_table(file_spec=file_spec, data_dir=None,
                                  paths=None,
                                  metadata=temp_meta_file.name)

    # make sure values are floats
    df_test = pd.DataFrame({'id': [11, 12], 'value_entry': [0, 0],
                            'value_exit': [1, np.NaN]})

    # sort because column order is not assured because started with dicts
    df = df.sort_index(axis=1)
    df_test = df_test.sort_index(axis=1)
    pdt.assert_frame_equal(df, df_test)

    # test error checking
    temp_meta_file2 = tempfile.NamedTemporaryFile(mode='w')
    metadata = {'name': 'test',
                'duplicate_check_columns': ['id', 'stage', 'value'],
                'columns_to_drop': ['years'],
                'categorical_var': ['value']}
    metadata_json = json.dumps(metadata)
    temp_meta_file2.file.write(metadata_json)
    temp_meta_file2.seek(0)
    assert_raises(ValueError, pk.read_entry_exit_table, file_spec=file_spec,
                  metadata=temp_meta_file2.name)

    temp_csv_file.close()
    temp_meta_file.close()
    temp_meta_file2.close()


def test_get_enrollment():
    """Test get_enrollment function."""
    # create temporary csv file & metadata file to read in
    temp_csv_file = tempfile.NamedTemporaryFile(mode='w')
    temp_meta_file = tempfile.NamedTemporaryFile(mode='w')
    df = pd.DataFrame({'id': [1, 1, 2, 2],
                       'time1': ['2001-01-13', '2004-05-21', '2003-06-10',
                                 '2003-06-10'], 'drop1': [2, 3, 4, 5],
                       'ig_dedup1': [5, 6, 7, 8], 'categ1': [0, 8, 0, 0]})
    df.to_csv(temp_csv_file, index=False)
    temp_csv_file.seek(0)

    metadata = ({'name': 'test',
                 'person_enrollment_ID': 'id',
                 'person_ID': 'id',
                 'program_ID': 'id',
                 'duplicate_check_columns': ['id', 'time1', 'categ1'],
                 'columns_to_drop': ['drop1'],
                 'categorical_var': ['categ1'], 'time_var': ['time1'],
                 'groupID_column': 'id'
                 })
    metadata_json = json.dumps(metadata)
    temp_meta_file.file.write(metadata_json)
    temp_meta_file.seek(0)

    file_spec = {2011: temp_csv_file.name}

    # first try with groups=True (default)
    df = pk.get_enrollment(file_spec=file_spec, data_dir=None, paths=None,
                           metadata_file=temp_meta_file.name)

    df_test = pd.DataFrame({'id': [1, 1], 'time1':
                            pd.to_datetime(['2001-01-13', '2004-05-21'],
                            errors='coerce'), 'ig_dedup1': [5, 6],
                            'categ1': [0, np.nan],
                            'years': [2011, 2011]})
    pdt.assert_frame_equal(df, df_test)

    # try again with groups=False
    df = pk.get_enrollment(groups=False, file_spec=file_spec, data_dir=None,
                           paths=None, metadata_file=temp_meta_file.name)

    df_test = pd.DataFrame({'id': [1, 1, 2],
                            'time1':
                            pd.to_datetime(['2001-01-13', '2004-05-21',
                                            '2003-06-10'], errors='coerce'),
                                'ig_dedup1': [5, 6, 8],
                                'categ1': [0, np.nan, 0],
                                'years': [2011, 2011, 2011]})
    # Have to change the index to match the one we de-duplicated
    df_test.index = pd.Int64Index([0, 1, 3])
    pdt.assert_frame_equal(df, df_test)

    temp_csv_file.close()
    temp_meta_file.close()


def test_get_exit():
    """test get_exit function."""
    # create temporary csv file & metadata file to read in
    temp_csv_file = tempfile.NamedTemporaryFile(mode='w')
    temp_meta_file = tempfile.NamedTemporaryFile(mode='w')
    dest_rand_ints = np.random.random_integers(1, 30, 3)
    df_init = pd.DataFrame({'id': [11, 12, 13], 'dest': dest_rand_ints})
    df_init.to_csv(temp_csv_file, index=False)
    temp_csv_file.seek(0)

    metadata = ({'name': 'test', 'duplicate_check_columns': ['id'],
                 "destination_column": 'dest', 'person_enrollment_ID': ['id']})
    metadata_json = json.dumps(metadata)
    temp_meta_file.file.write(metadata_json)
    temp_meta_file.seek(0)

    file_spec = {2011: temp_csv_file.name}

    df = pk.get_exit(file_spec=file_spec, data_dir=None, paths=None,
                     metadata_file=temp_meta_file.name)

    mapping_table = pd.read_csv(op.join(puget.data.DATA_PATH, 'metadata',
                                        'destination_mappings.csv'))

    map_table_test_ints = [2, 25, 26]
    map_table_test = pd.DataFrame({'Standard': np.array(['New Standards']*3),
                                   'DestinationNumeric': np.array(
                                        map_table_test_ints).astype(float),
                                   'DestinationDescription': ['Transitional housing for homeless persons (including homeless youth)',
                                                              'Long-term care facility or nursing home',
                                                              'Moved from one HOPWA funded project to HOPWA PH'],
                                   'DestinationGroup': ['Temporary',
                                                        'Permanent',
                                                        'Permanent'],
                                   'DestinationSuccess': ['Other Exit',
                                                          'Successful Exit',
                                                          'Successful Exit'],
                                   'Subsidy': ['No', 'No', 'Yes']})

    map_table_subset = mapping_table[mapping_table['DestinationNumeric'] ==
                                     map_table_test_ints[0]]
    map_table_subset = map_table_subset.append(mapping_table[
        mapping_table['DestinationNumeric'] == map_table_test_ints[1]])
    map_table_subset = map_table_subset.append(mapping_table[
        mapping_table['DestinationNumeric'] == map_table_test_ints[2]])
    # Have to change the index to match the one we made up
    map_table_subset.index = pd.Int64Index([0, 1, 2])

    # sort because column order is not assured because started with dicts
    map_table_test = map_table_test.sort_index(axis=1)
    map_table_subset = map_table_subset.sort_index(axis=1)

    pdt.assert_frame_equal(map_table_subset, map_table_test)

    mapping_table = mapping_table[mapping_table.Standard == 'New Standards']
    mapping_table['Subsidy'] = mapping_table['Subsidy'].map({'Yes': True,
                                                             'No': False})
    mapping_table = mapping_table.drop(['Standard'], axis=1)

    df_test = pd.DataFrame({'id': [11, 12, 13],
                            'years': [2011, 2011, 2011],
                            'dest': dest_rand_ints})
    df_test = pd.merge(left=df_test, right=mapping_table, how='left',
                       left_on='dest', right_on='DestinationNumeric')
    df_test = df_test.drop('dest', axis=1)

    pdt.assert_frame_equal(df, df_test)

    temp_csv_file.close()
    temp_meta_file.close()


def test_get_client():
    # create temporary csv files & metadata file to read in
    temp_csv_file1 = tempfile.NamedTemporaryFile(mode='w')
    temp_csv_file2 = tempfile.NamedTemporaryFile(mode='w')
    df_init = pd.DataFrame({'id': [11, 12, 13, 15, 16, 17],
                            'dob_col': ['1990-01-13', '2012-05-21',
                                        '1850-06-14', '1965-11-22',
                                        '1948-09-03', '2012-03-18'],
                            'bool_col': [1, 99, 1, 8, 0, 1],
                            'numeric': [99, 3, 6, 0, 8, np.NaN]})
    df2_init = pd.DataFrame({'id': [11, 12, 13, 14, 15, 16, 17, 18],
                             'dob_col': ['1990-01-15', '2012-05-21',
                                         '1850-06-14', '1975-12-08',
                                         '1967-11-22', pd.NaT, '2010-03-18',
                                         '2014-04-30'],
                             'bool_col': [0, 0, 1, 0, 8, 0, np.NaN, 1],
                             'numeric': [5, 3, 7, 1, 0, 8, 6, 0]})
    df_init.to_csv(temp_csv_file1, index=False)
    temp_csv_file1.seek(0)
    df2_init.to_csv(temp_csv_file2, index=False)
    temp_csv_file2.seek(0)

    years = [2011, 2013]
    file_spec = dict(zip(years, [temp_csv_file1.name, temp_csv_file2.name]))

    temp_meta_file = tempfile.NamedTemporaryFile(mode='w')
    metadata = ({'name': 'test', 'person_ID': 'id',
                 'duplicate_check_columns': ['id'],
                 'categorical_var': ['bool_col', 'numeric'],
                 'time_var': ['dob_col'],
                 'boolean': ['bool_col'], 'numeric_code': ['numeric'],
                 'dob_column': 'dob_col'})
    metadata_json = json.dumps(metadata)
    temp_meta_file.file.write(metadata_json)
    temp_meta_file.seek(0)

    # get path & filenames
    df = pk.get_client(file_spec=file_spec, data_dir=None, paths=None,
                       years=years, metadata_file=temp_meta_file.name)

    df_test = pd.DataFrame({'id': [11, 12, 13, 14, 15, 16, 17, 18],
                            'dob_col': ['1990-01-14', pd.NaT, pd.NaT,
                                        '1975-12-08', pd.NaT, '1948-09-03',
                                        '2010-03-18', pd.NaT],
                            'bool_col': [1, 0, 1, 0, np.NaN, 0, 1, 1],
                            'numeric': [5, 3, np.NaN, 1, 0, np.NaN, 6, 0]})

    # Have to change the index to match the one we de-duplicated
    df_test.index = pd.Int64Index([6, 7, 8, 9, 10, 11, 12, 13])
    pdt.assert_frame_equal(df, df_test)

    # test error checking
    temp_meta_file2 = tempfile.NamedTemporaryFile(mode='w')
    metadata = ({'name': 'test',
                 'duplicate_check_columns': ['id'],
                 'categorical_var': ['bool_col', 'numeric'],
                 'time_var': ['dob_col'],
                 'boolean': ['bool_col'], 'numeric_code': ['numeric'],
                 'dob_column': 'dob_col'})
    metadata_json = json.dumps(metadata)
    temp_meta_file2.file.write(metadata_json)
    temp_meta_file2.seek(0)
    assert_raises(ValueError, pk.get_client,
                  file_spec=file_spec, data_dir=None, paths=None,
                  metadata_file=temp_meta_file2.name)

    temp_csv_file1.close()
    temp_csv_file2.close()
    temp_meta_file.close()


def test_get_disabilities():
    temp_csv_file = tempfile.NamedTemporaryFile(mode='w')
    df_init = pd.DataFrame({'pid': [11, 11, 11, 11, 12, 12, 12, 12],
                            'stage': [10, 10, 20, 20, 10, 10, 20, 20],
                            'type': [5, 6, 5, 6, 5, 6, 5, 6],
                            'response': [0, 1, 0, 1, 99, 0, 0, 1]})
    df_init.to_csv(temp_csv_file, index=False)
    temp_csv_file.seek(0)

    temp_meta_file = tempfile.NamedTemporaryFile(mode='w')
    metadata = {'name': 'test',
                'duplicate_check_columns': ['pid', 'stage', 'type'],
                'columns_to_drop': ['years'],
                'categorical_var': ['response'],
                'collection_stage_column': 'stage', 'entry_stage_val': 10,
                'exit_stage_val': 20, 'update_stage_val': 30,
                'type_column': 'type', 'response_column': 'response',
                'person_enrollment_ID': 'pid'}

    metadata_json = json.dumps(metadata)
    temp_meta_file.file.write(metadata_json)
    temp_meta_file.seek(0)

    file_spec = {2011: temp_csv_file.name}

    df = pk.get_disabilities(file_spec=file_spec, data_dir=None, paths=None,
                             metadata_file=temp_meta_file.name)

    type_dict = {5: 'Physical', 6: 'Developmental', 7: 'ChronicHealth',
                 8: 'HIVAIDS', 9: 'MentalHealth', 10: 'SubstanceAbuse'}

    # make sure values are floats
    df_test = pd.DataFrame({'pid': [11, 12], 'Physical_entry': [0, np.NaN],
                            'Physical_exit': [0.0, 0.0],
                            'Developmental_entry': [1.0, 0.0],
                            'Developmental_exit': [1.0, 1.0]})

    # sort because column order is not assured because started with dicts
    df = df.sort_index(axis=1)
    df_test = df_test.sort_index(axis=1)
    pdt.assert_frame_equal(df, df_test)

    # test error checking
    temp_meta_file2 = tempfile.NamedTemporaryFile(mode='w')
    metadata = {'name': 'test',
                'duplicate_check_columns': ['pid', 'stage', 'type'],
                'categorical_var': ['response']}
    metadata_json = json.dumps(metadata)
    temp_meta_file2.file.write(metadata_json)
    temp_meta_file2.seek(0)
    assert_raises(ValueError, pk.get_disabilities, file_spec=file_spec,
                  data_dir=None, paths=None,
                  metadata_file=temp_meta_file2.name)

    temp_csv_file.close()
    temp_meta_file.close()
    temp_meta_file2.close()


def test_get_employment_education():
    temp_csv_file = tempfile.NamedTemporaryFile(mode='w')
    df_init = pd.DataFrame({'id': [11, 11, 12],
                            'stage': [0, 1, 0], 'value': [0, 1, 0]})
    df_init.to_csv(temp_csv_file, index=False)
    temp_csv_file.seek(0)

    temp_meta_file = tempfile.NamedTemporaryFile(mode='w')
    metadata = {'name': 'test',
                'duplicate_check_columns': ['id', 'stage', 'value'],
                'columns_to_drop': ['years'],
                'categorical_var': ['value'],
                'collection_stage_column': 'stage', 'entry_stage_val': 0,
                'exit_stage_val': 1, 'update_stage_val': 2,
                'person_enrollment_ID': 'id'}

    metadata_json = json.dumps(metadata)
    temp_meta_file.file.write(metadata_json)
    temp_meta_file.seek(0)

    file_spec = {2011: temp_csv_file.name}

    df = pk.get_employment_education(file_spec=file_spec, data_dir=None,
                                     paths=None,
                                     metadata_file=temp_meta_file.name)

    # make sure values are floats
    df_test = pd.DataFrame({'id': [11, 12], 'value_entry': [0, 0],
                            'value_exit': [1, np.NaN]})

    # sort because column order is not assured because started with dicts
    df = df.sort_index(axis=1)
    df_test = df_test.sort_index(axis=1)
    pdt.assert_frame_equal(df, df_test)

    temp_csv_file.close()
    temp_meta_file.close()


def test_get_health_dv():
    temp_csv_file = tempfile.NamedTemporaryFile(mode='w')
    df_init = pd.DataFrame({'id': [11, 11, 12],
                            'stage': [0, 1, 0], 'value': [0, 1, 0]})
    df_init.to_csv(temp_csv_file, index=False)
    temp_csv_file.seek(0)

    temp_meta_file = tempfile.NamedTemporaryFile(mode='w')
    metadata = {'name': 'test',
                'duplicate_check_columns': ['id', 'stage', 'value'],
                'columns_to_drop': ['years'],
                'categorical_var': ['value'],
                'collection_stage_column': 'stage', 'entry_stage_val': 0,
                'exit_stage_val': 1, 'update_stage_val': 2,
                'person_enrollment_ID': 'id'}

    metadata_json = json.dumps(metadata)
    temp_meta_file.file.write(metadata_json)
    temp_meta_file.seek(0)

    file_spec = {2011: temp_csv_file.name}

    df = pk.get_health_dv(file_spec=file_spec, data_dir=None, paths=None,
                          metadata_file=temp_meta_file.name)

    # make sure values are floats
    df_test = pd.DataFrame({'id': [11, 12], 'value_entry': [0, 0],
                            'value_exit': [1, np.NaN]})

    # sort because column order is not assured because started with dicts
    df = df.sort_index(axis=1)
    df_test = df_test.sort_index(axis=1)
    pdt.assert_frame_equal(df, df_test)

    temp_csv_file.close()
    temp_meta_file.close()


def test_get_income():
    temp_csv_file = tempfile.NamedTemporaryFile(mode='w')
    df_init = pd.DataFrame({'pid': [11, 11, 11, 12, 12, 12, 12],
                            'stage': [0, 0, 1, 0, 0, 1, 1],
                            'income': [1, 1, 1, 0, 1, np.NaN, 1],
                            'incomeAmount': [5, 8, 12, 0, 6, 0, 3]})
    df_init.to_csv(temp_csv_file, index=False)
    temp_csv_file.seek(0)

    temp_meta_file = tempfile.NamedTemporaryFile(mode='w')
    metadata = {'name': 'test',
                'duplicate_check_columns': ['pid', 'stage', 'income',
                                            'incomeAmount'],
                'columns_to_drop': ['years'],
                'categorical_var': ['income'],
                'collection_stage_column': 'stage', 'entry_stage_val': 0,
                'exit_stage_val': 1, 'update_stage_val': 2,
                'person_enrollment_ID': 'pid',
                'columns_to_take_max': ['income', 'incomeAmount']}
    metadata_json = json.dumps(metadata)
    temp_meta_file.file.write(metadata_json)
    temp_meta_file.seek(0)

    file_spec = {2011: temp_csv_file.name}

    df = pk.get_income(file_spec=file_spec, data_dir=None, paths=None,
                       metadata_file=temp_meta_file.name)

    df_test = pd.DataFrame({'pid': [11, 12],
                            'income_entry': [1.0, 1.0],
                            'income_exit': [1.0, 1.0],
                            'incomeAmount_entry': [8, 6],
                            'incomeAmount_exit': [12, 3]})
    # Have to change the index to match the one we de-duplicated
    df_test.index = pd.Int64Index([0, 2])
    # sort because column order is not assured because started with dicts
    df = df.sort_index(axis=1)
    df_test = df_test.sort_index(axis=1)

    pdt.assert_frame_equal(df, df_test)

    # test error checking
    temp_meta_file2 = tempfile.NamedTemporaryFile(mode='w')
    metadata = {'name': 'test',
                'duplicate_check_columns': ['pid', 'stage', 'type'],
                'categorical_var': ['response']}
    metadata_json = json.dumps(metadata)
    temp_meta_file2.file.write(metadata_json)
    temp_meta_file2.seek(0)
    assert_raises(ValueError, pk.get_income, file_spec=file_spec,
                  data_dir=None, paths=None,
                  metadata_file=temp_meta_file2.name)

    temp_csv_file.close()
    temp_meta_file.close()
    temp_meta_file2.close()


def test_get_project():
    temp_csv_file = tempfile.NamedTemporaryFile(mode='w')
    df_init = pd.DataFrame({'pid': [3, 4], 'name': ['shelter1', 'rrh2'],
                            'ProjectType': [1, 13]})
    df_init.to_csv(temp_csv_file, index=False)
    temp_csv_file.seek(0)

    temp_meta_file = tempfile.NamedTemporaryFile(mode='w')
    metadata = {'name': 'test', 'program_ID': 'pid',
                'duplicate_check_columns': ['pid', 'name', 'ProjectType'],
                'columns_to_drop': ['years'],
                'project_type_column': 'ProjectType'}

    metadata_json = json.dumps(metadata)
    temp_meta_file.file.write(metadata_json)
    temp_meta_file.seek(0)

    file_spec = {2011: temp_csv_file.name}

    df = pk.get_project(file_spec=file_spec, data_dir=None, paths=None,
                        metadata_file=temp_meta_file.name)

    df_test = pd.DataFrame({'pid': [3, 4], 'name': ['shelter1', 'rrh2'],
                            'ProjectNumeric': [1, 13],
                            'ProjectType': ['Emergency Shelter',
                                            'PH - Rapid Re-Housing']})

    # sort because column order is not assured because started with dicts
    df = df.sort_index(axis=1)
    df_test = df_test.sort_index(axis=1)
    pdt.assert_frame_equal(df, df_test)


def test_merge():

    with tempfile.TemporaryDirectory() as temp_dir:
        year_str = '2011'
        paths = {2011: year_str}
        dir_year = op.join(temp_dir, year_str)
        os.makedirs(dir_year, exist_ok=True)
        # make up all the csv files and metadata files
        enrollment_df = pd.DataFrame({'personID': [1, 2],
                                      'person_enrollID': [10, 20],
                                      'programID': [100, 200],
                                      'groupID': [1000, 2000],
                                      'entrydate': ['2011-01-13',
                                                    '2011-06-10']})
        # print(enrollment_df)
        enrollment_metadata = {'name': 'enrollment',
                               'person_enrollment_ID': 'person_enrollID',
                               'person_ID': 'personID',
                               'program_ID': 'programID',
                               'groupID_column': 'groupID',
                               'duplicate_check_columns': ['personID',
                                                           'person_enrollID',
                                                           'programID',
                                                           'groupID'],
                               'columns_to_drop': ['years'],
                               'time_var': ['entrydate']}
        enrollment_csv_file = op.join(dir_year, 'Enrollment.csv')
        enrollment_df.to_csv(enrollment_csv_file, index=False)
        enrollment_meta_file = op.join(dir_year, 'Enrollment.json')
        with open(enrollment_meta_file, 'w') as outfile:
            json.dump(enrollment_metadata, outfile)

        exit_df = pd.DataFrame({'ppid': [10, 20],
                                'dest_num': [12, 27],
                                'exitdate': ['2011-08-01', '2011-12-21']})
        exit_metadata = {'name': 'exit', 'person_enrollment_ID': 'ppid',
                         'destination_column': 'dest_num',
                         'duplicate_check_columns': ['ppid'],
                         'columns_to_drop': ['years'],
                         'time_var': ['exitdate']}
        exit_csv_file = op.join(dir_year, 'Exit.csv')
        exit_df.to_csv(exit_csv_file, index=False)
        exit_meta_file = op.join(dir_year, 'Exit.json')
        with open(exit_meta_file, 'w') as outfile:
            json.dump(exit_metadata, outfile)

        client_df = pd.DataFrame({'pid': [1, 2],
                                  'dob': ['1990-03-13', '1955-08-21'],
                                  'gender': [0, 1],
                                  'veteran': [0, 1]})
        client_metadata = {'name': 'client', 'person_ID': 'pid',
                           'dob_column': 'dob',
                           'time_var': ['dob'],
                           'categorical_var': ['gender', 'veteran'],
                           'boolean': ['veteran'],
                           'numeric_code': ['gender'],
                           'duplicate_check_columns': ['pid']}
        client_csv_file = op.join(dir_year, 'Client.csv')
        client_df.to_csv(client_csv_file, index=False)
        client_meta_file = op.join(dir_year, 'Client.json')
        with open(client_meta_file, 'w') as outfile:
            json.dump(client_metadata, outfile)

        disabilities_df = pd.DataFrame({'person_enrollID': [10, 10, 20, 20],
                                        'stage': [0, 1, 0, 1],
                                        'type': [5, 5, 5, 5],
                                        'response': [0, 0, 1, 1]})
        disabilities_metadata = {'name': 'disabilities',
                                 'person_enrollment_ID': 'person_enrollID',
                                 'categorical_var': ['response'],
                                 'collection_stage_column': 'stage',
                                 'entry_stage_val': 0, "exit_stage_val": 1,
                                 'update_stage_val': 2, 'type_column': 'type',
                                 'response_column': 'response',
                                 'duplicate_check_columns': ['person_enrollID',
                                                             'stage', 'type'],
                                 'columns_to_drop': ['years']}

        disabilities_csv_file = op.join(dir_year, 'Disabilities.csv')
        disabilities_df.to_csv(disabilities_csv_file, index=False)
        disabilities_meta_file = op.join(dir_year, 'Disabilities.json')
        with open(disabilities_meta_file, 'w') as outfile:
            json.dump(disabilities_metadata, outfile)

        emp_edu_df = pd.DataFrame({'ppid': [10, 10, 20, 20],
                                   'stage': [0, 1, 0, 1],
                                   'employed': [0, 0, 0, 1]})
        emp_edu_metadata = {'name': 'employment_education',
                            'person_enrollment_ID': 'ppid',
                            'categorical_var': ['employed'],
                            'collection_stage_column': 'stage',
                            'entry_stage_val': 0, "exit_stage_val": 1,
                            'update_stage_val': 2,
                            'duplicate_check_columns': ['ppid', 'stage'],
                            'columns_to_drop': ['years']}

        emp_edu_csv_file = op.join(dir_year, 'EmploymentEducation.csv')
        emp_edu_df.to_csv(emp_edu_csv_file, index=False)
        emp_edu_meta_file = op.join(dir_year, 'EmploymentEducation.json')
        with open(emp_edu_meta_file, 'w') as outfile:
            json.dump(emp_edu_metadata, outfile)

        health_dv_df = pd.DataFrame({'ppid': [10, 10, 20, 20],
                                     'stage': [0, 1, 0, 1],
                                     'health_status': [0, 0, 0, 1]})
        health_dv_metadata = {'name': 'health_dv',
                              'person_enrollment_ID': 'ppid',
                              'categorical_var': ['health_status'],
                              'collection_stage_column': 'stage',
                              'entry_stage_val': 0, 'exit_stage_val': 1,
                              'update_stage_val': 2,
                              'duplicate_check_columns': ['ppid', 'stage'],
                              'columns_to_drop': ['years']}
        health_dv_csv_file = op.join(dir_year, 'HealthAndDV.csv')
        health_dv_df.to_csv(health_dv_csv_file, index=False)
        health_dv_meta_file = op.join(dir_year, 'HealthAndDV.json')
        with open(health_dv_meta_file, 'w') as outfile:
            json.dump(health_dv_metadata, outfile)

        income_df = pd.DataFrame({'ppid': [10, 10, 20, 20],
                                  'stage': [0, 1, 0, 1],
                                  'income': [0, 0, 0, 1000]})
        income_metadata = {'name': 'income', 'person_enrollment_ID': 'ppid',
                           'categorical_var': ['income'],
                           'collection_stage_column': 'stage',
                           'entry_stage_val': 0, 'exit_stage_val': 1,
                           'update_stage_val': 2,
                           'columns_to_take_max': ['income'],
                           'duplicate_check_columns': ['ppid', 'stage'],
                           'columns_to_drop': ['years']}

        income_csv_file = op.join(dir_year, 'IncomeBenefits.csv')
        income_df.to_csv(income_csv_file, index=False)
        income_meta_file = op.join(dir_year, 'IncomeBenefits.json')
        with open(income_meta_file, 'w') as outfile:
            json.dump(income_metadata, outfile)

        project_df = pd.DataFrame({'pr_id': [100, 200],
                                   'type': [1, 2]})
        project_metadata = {'name': 'project', 'program_ID': 'pr_id',
                            'project_type_column': 'type',
                            'duplicate_check_columns': ['pr_id'],
                            'columns_to_drop': ['years']}

        project_csv_file = op.join(dir_year, 'Project.csv')
        project_df.to_csv(project_csv_file, index=False)
        project_meta_file = op.join(dir_year, 'Project.json')
        with open(project_meta_file, 'w') as outfile:
            json.dump(project_metadata, outfile)

        metadata_files = {'enrollment': enrollment_meta_file,
                          'exit': exit_meta_file,
                          'client': client_meta_file,
                          'disabilities': disabilities_meta_file,
                          'employment_education': emp_edu_meta_file,
                          'health_dv': health_dv_meta_file,
                          'income': income_meta_file,
                          'project': project_meta_file}

        df = pk.merge_tables(meta_files=metadata_files,
                             data_dir=temp_dir, paths=paths, groups=False)

        df_test = pd.DataFrame({'personID': [1, 2],
                                'person_enrollID': [10, 20],
                                'programID': [100, 200],
                                'groupID': [1000, 2000],
                                'entrydate': pd.to_datetime(['2011-01-13',
                                                             '2011-06-10']),
                                'DestinationNumeric': [12., 27.],
                                'DestinationDescription': ['Staying or living with family, temporary tenure (e.g., room, apartment or house)',
                                                           'Moved from one HOPWA funded project to HOPWA TH'],
                                'DestinationGroup': ['Temporary', 'Temporary'],
                                'DestinationSuccess': ['Other Exit',
                                                       'Other Exit'],
                                'exitdate': pd.to_datetime(['2011-08-01',
                                                            '2011-12-21']),
                                'Subsidy': [False, False],
                                'dob': pd.to_datetime(['1990-03-13',
                                                       '1955-08-21']),
                                'gender': [0, 1],
                                'veteran': [0, 1],
                                'Physical_entry': [0, 1],
                                'Physical_exit': [0, 1],
                                'employed_entry': [0, 0],
                                'employed_exit': [0, 1],
                                'health_status_entry': [0, 0],
                                'health_status_exit': [0, 1],
                                'income_entry': [0, 0],
                                'income_exit': [0, 1000],
                                'ProjectNumeric': [1, 2],
                                'ProjectType': ['Emergency Shelter',
                                                'Transitional Housing']})

        # sort because column order is not assured because started with dicts
        df = df.sort_index(axis=1)
        df_test = df_test.sort_index(axis=1)
        pdt.assert_frame_equal(df, df_test)
