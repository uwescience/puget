
import numpy as np
import numpy.testing as npt

import pandas as pd
import pandas.util.testing as pdt

import puget.cluster as cluster


def test_make_mapping():
    pass


def test_cluster_by_groups():
    # In the first case, all individuals are linked through a chain of
    # co-occurrences:
    df1 = pd.DataFrame({'individual_var': [1, 200, 3, 100, 1, 200, 3, 100],
                        'group_var': [1, 1, 2, 2, 1, 2, 1, 2]})

    T = cluster.groups_co_occurrence(df1, 'individual_var', 'group_var')
    true_T = np.array([[0, 1, 1, 0], [1, 0, 2, 1], [1, 2, 0, 1], [0, 1, 1, 0]])
    npt.assert_equal(T, true_T)

    # Individual IDs are arbitrary (can be 100, 200, etc.):
    df1_out = cluster.cluster(df1, 'individual_var', group_var='group_var')
    true_df1_out = pd.DataFrame({'individual_var': [1, 200, 3, 100,
                                                    1, 200, 3, 100],
                                 'group_var': [1, 1, 2, 2, 1, 2, 1, 2],
                                 'cluster': [1, 1, 1, 1, 1, 1, 1, 1]})

    pdt.assert_frame_equal(df1_out.sort_index(axis=1),
                           true_df1_out.sort_index(axis=1))

    #  In the second case, individuals are unlinked into two clusters:
    df2 = pd.DataFrame({'individual_var': [1, 2, 3, 4, 1, 2, 3, 4],
                        'group_var': [1, 1, 3, 3, 1, 1, 3, 3]})

    T = cluster.groups_co_occurrence(df2, 'individual_var', 'group_var')
    true_T = np.array([[0, 1, 0, 0], [1, 0, 0, 0], [0, 0, 0, 1], [0, 0, 1, 0]])
    T = cluster.groups_co_occurrence(df2, 'individual_var', 'group_var')
    npt.assert_equal(T, true_T)

    #  Cluster's are designated as [1, 2, 3, ...], even while the group
    # variable can have arbitrary values (e.g., [1, 3, 100003, ...]):
    df2_out = cluster.cluster(df2, 'individual_var', group_var='group_var')
    true_df2_out = pd.DataFrame({'individual_var': [1, 2, 3, 4, 1, 2, 3, 4],
                                 'group_var': [1, 1, 3, 3, 1, 1, 3, 3],
                                 'cluster': [1, 1, 2, 2, 1, 1, 2, 2]})

    pdt.assert_frame_equal(df2_out.sort_index(axis=1),
                           true_df2_out.sort_index(axis=1))


def test_cluster_by_time():
    df1 = pd.DataFrame({'individual_var': [1, 200, 3, 100, 1, 200, 3, 100],
                        'time_var1': pd.to_datetime(['2001-01-13',
                                                     '2001-01-13',
                                                     '2003-06-10',
                                                     '2003-06-10',
                                                     '2001-01-13',
                                                     '2001-01-13',
                                                     '2003-06-10',
                                                     '2003-06-10']),
                        'time_var2': pd.to_datetime(['2001-01-13',
                                                     '2003-06-10',
                                                     '2001-01-13',
                                                     '2003-06-10',
                                                     '2001-01-13',
                                                     '2003-06-10',
                                                     '2001-01-13',
                                                     '2003-06-10'])})

    T = cluster.time_co_occurrence(df1, 'individual_var', ['time_var1'])
    true_T = np.array([[0, 1, 0, 0], [1, 0, 0, 0], [0, 0, 0, 1], [0, 0, 1, 0]])
    npt.assert_equal(T, true_T)

    # The first test-case usese only one time variable to establish linkage:
    df1_out = cluster.cluster(df1, 'individual_var', time_var=['time_var1'])
    true_df1_out = pd.DataFrame({'individual_var': [1, 200, 3, 100,
                                                    1, 200, 3, 100],
                                 'time_var1': pd.to_datetime(['2001-01-13',
                                                              '2001-01-13',
                                                              '2003-06-10',
                                                              '2003-06-10',
                                                              '2001-01-13',
                                                              '2001-01-13',
                                                              '2003-06-10',
                                                              '2003-06-10']),
                                'time_var2': pd.to_datetime(['2001-01-13',
                                                             '2003-06-10',
                                                             '2001-01-13',
                                                             '2003-06-10',
                                                             '2001-01-13',
                                                             '2003-06-10',
                                                             '2001-01-13',
                                                             '2003-06-10']),
                                 'cluster': [1, 1, 2, 2, 1, 1, 2, 2]})

    pdt.assert_frame_equal(df1_out.sort_index(axis=1),
                           true_df1_out.sort_index(axis=1))

    # In the second test-case, all individuals are linked through a crossing
    # Of the two different time-variables used for clustering:
    T = cluster.time_co_occurrence(df1, 'individual_var', ['time_var1',
                                                           'time_var2'])

    true_T = np.array([[0, 1, 1, 0], [1, 0, 0, 1], [1, 0, 0, 1], [0, 1, 1, 0]])
    npt.assert_equal(T, true_T)

    df1_out = cluster.cluster(df1, 'individual_var', time_var=['time_var1',
                                                               'time_var2'])

    true_df1_out = pd.DataFrame({'individual_var': [1, 200, 3, 100,
                                                    1, 200, 3, 100],
                                 'time_var1': pd.to_datetime(['2001-01-13',
                                                              '2001-01-13',
                                                              '2003-06-10',
                                                              '2003-06-10',
                                                              '2001-01-13',
                                                              '2001-01-13',
                                                              '2003-06-10',
                                                              '2003-06-10']),
                                'time_var2': pd.to_datetime(['2001-01-13',
                                                             '2003-06-10',
                                                             '2001-01-13',
                                                             '2003-06-10',
                                                             '2001-01-13',
                                                             '2003-06-10',
                                                             '2001-01-13',
                                                             '2003-06-10']),
                                 'cluster': [1, 1, 1, 1, 1, 1, 1, 1]})
