"""Tests for functions in king.py."""
import puget.king as pk
import puget
import os.path as op
import pandas as pd
import pandas.util.testing as pdt
import numpy as np
import tempfile
from nose import with_setup

TF = tempfile.NamedTemporaryFile()


def setup():
    """Create temporary file for testing."""
    df = pd.DataFrame({'time1': ['2001-01-13', '2004-05-21'], 'drop1': [2, 3],
                       'ig_dedup1': [5, 6], 'categ1': [0, 8]})
    df.to_csv(TF)
    TF.seek(0)


def teardown():
    """Close temporary file."""
    TF.close()


@with_setup(setup, teardown)
def test_read_table():
    """Test read_table function."""
    path, fname = op.split(TF.name)
    path0, path1 = op.split(path)
    path_dict = {2011: path1}
    df = pk.read_table(fname, path0, years=2011, paths=path_dict,
                       columns_to_drop=['drop1'],
                       categorical_var=['categ1'],
                       time_var=['time1'])
    df_test = pd.DataFrame({'Unnamed: 0': [0, 1], 'time1':
                            pd.to_datetime(['2001-01-13', '2004-05-21'],
                            coerce=True), 'ig_dedup1': [5, 6],
                            'categ1': [0, np.nan],
                            'years': [2011, 2011]})
    pdt.assert_frame_equal(df, df_test)


def test_read_enrollment():
    """Test get_enrollment function."""
    df_hard = pk.get_enrollment(False)
    df_new = pk.get_enrollment(True)

    pdt.assert_frame_equal(df_hard, df_new)
