import puget.utils as pu
import puget
import os.path as op
import pandas as pd
import pandas.util.testing as pdt
import tempfile


def test_merge_destination():
    """Test merge_destination function."""
    TF = tempfile.NamedTemporaryFile(mode='w')
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
    df.to_csv(TF, index=False)
    TF.seek(0)

    path, fname = op.split(TF.name)
    df = pd.DataFrame({'numeric': [1, 2, 3]})
    df_merge = pu.merge_destination(df, df_destination_column='numeric',
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

    TF.close()
