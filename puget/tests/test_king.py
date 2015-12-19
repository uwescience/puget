"""Tests for functions in king.py."""
import puget.king as pk
import puget
import os.path as op
import pandas as pd
import pandas.util.testing as pdt
import numpy as np
import tempfile
import json
from nose import with_setup


class TestFunctions(object):
    """Class for testing king.py with setup and teardown methods."""

    @classmethod
    def setup_class(cls):
        """Create temporary file for testing."""
        cls.TF = tempfile.NamedTemporaryFile()
        cls.TF2 = tempfile.NamedTemporaryFile()
        df = pd.DataFrame({'id': [1, 1, 2, 2],
                           'time1': ['2001-01-13', '2004-05-21', '2003-06-10',
                                     '2003-06-10'],
                           'drop1': [2, 3, 4, 5],
                           'ig_dedup1': [5, 6, 7, 8],
                           'categ1': [0, 8, 0, 0]})
        df.to_csv(cls.TF)
        cls.TF.seek(0)

        metadata = ({'name': 'test',
                     'duplicate_check_columns': ['id', 'time1', 'categ1'],
                     'columns_to_drop': ['drop1'],
                     'categorical_var': ['categ1'], 'time_var': ['time1']})
        metadata_json = json.dumps(metadata)
        cls.TF2.file.write(metadata_json)
        cls.TF2.seek(0)

    @classmethod
    def teardown_class(cls):
        """Close temporary file."""
        cls.TF.close()
        cls.TF2.close()

    def test_read_table(self):
        """Test read_table function."""
        path, fname = op.split(self.TF.name)
        path0, path1 = op.split(path)
        path_dict = {2011: path1}
        df = pk.read_table(fname, path0, years=2011, paths=path_dict,
                           columns_to_drop=['drop1'],
                           categorical_var=['categ1'],
                           time_var=['time1'],
                           duplicate_check_columns=['id', 'time1', 'categ1'])
        df_test = pd.DataFrame({'Unnamed: 0': [0, 1, 3], 'id': [1, 1, 2],
                                'time1':
                                pd.to_datetime(['2001-01-13', '2004-05-21',
                                                '2003-06-10'],
                                               errors='coerce'),
                                'ig_dedup1': [5, 6, 8],
                                'categ1': [0, np.nan, 0],
                                'years': [2011, 2011, 2011]})
        # Have to change the index to match the one we de-duplicated
        df_test.index = pd.Int64Index([0, 1, 3])
        pdt.assert_frame_equal(df, df_test)

    def test_get_enrollment(self):
        """Test get_enrollment function."""
        path, fname = op.split(self.TF.name)
        path0, path1 = op.split(path)
        path_dict = {2011: path1}
        # first try with groups=True (default)
        df = pk.get_enrollment(filename=fname, data_dir=path0, years=2011,
                               paths=path_dict, metadata_file=self.TF2.name,
                               groupID_column='id')

        df_test = pd.DataFrame({'Unnamed: 0': [0, 1], 'id': [1, 1], 'time1':
                                pd.to_datetime(['2001-01-13', '2004-05-21'],
                                errors='coerce'), 'ig_dedup1': [5, 6],
                                'categ1': [0, np.nan],
                                'years': [2011, 2011]})
        pdt.assert_frame_equal(df, df_test)

        # try again with groups=False
        df = pk.get_enrollment(groups=False, filename=fname, data_dir=path0,
                               years=2011, paths=path_dict,
                               metadata_file=self.TF2.name,
                               groupID_column='id')

        df_test = pd.DataFrame({'Unnamed: 0': [0, 1, 3], 'id': [1, 1, 2],
                                'time1':
                                pd.to_datetime(['2001-01-13', '2004-05-21',
                                                '2003-06-10'],
                                               errors='coerce'),
                                'ig_dedup1': [5, 6, 8],
                                'categ1': [0, np.nan, 0],
                                'years': [2011, 2011, 2011]})
        # Have to change the index to match the one we de-duplicated
        df_test.index = pd.Int64Index([0, 1, 3])
        pdt.assert_frame_equal(df, df_test)

# TODO: add test for get_client
