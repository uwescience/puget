import puget.utils as pu
import puget
import os.path as op
import pandas as pd
import pandas.util.testing as pdt
import numpy as np
import tempfile
from nose import with_setup


class TestFunctions(object):
    """Class for testing utils.py with setup and teardown methods."""

    @classmethod
    def setup_class(cls):
        """Create temporary file for testing."""
        cls.TF = tempfile.NamedTemporaryFile()
        cls.TF2 = tempfile.NamedTemporaryFile()
        df = pd.DataFrame({'Standard': ['New Standards', 'New Standards',
                                        'New Standards', 'Old Standards'],
                           'DestinationNumeric': [1, 2, 3, 4],
                           'DestinationDescription': ['Success no subsidy',
                                                      'Success with subsidy',
                                                      'Unsuccessful',
                                                      'Unsuccessful'],
                           'DestinationGroup': ['Permanent', 'Permanent',
                                                'Temporary', 'Temporary'],
                           'DestinationSuccess': ['Successful Exit',
                                                  'Successful Exit',
                                                  'Other Exit', 'Other Exit'],
                           'Subsidy': ['No', 'Yes', 'No', 'No']})
        df.to_csv(cls.TF, index=False)
        cls.TF.seek(0)

    @classmethod
    def teardown_class(cls):
        """Close temporary file."""
        cls.TF.close()
        cls.TF2.close()

    def test_merge_destination(self):
        """Test merge_destination function."""
        path, fname = op.split(self.TF.name)
        df = pd.DataFrame({'numeric': [1, 2, 3]})
        df_merge = pu.merge_destination(df, df_destination_colname='numeric',
                                        destination_map_fname=fname,
                                        directory=path)
        df_test = pd.DataFrame({'DestinationNumeric': [1, 2, 3],
                                'DestinationDescription': [
                                    'Success no subsidy',
                                    'Success with subsidy',
                                    'Unsuccessful'],
                                'DestinationGroup': ['Permanent', 'Permanent',
                                                     'Temporary'],
                                'DestinationSuccess': ['Successful Exit',
                                                       'Successful Exit',
                                                       'Other Exit'],
                                'Subsidy': [False, True, False]})
        pdt.assert_frame_equal(df_merge, df_test)
