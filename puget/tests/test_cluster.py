
import numpy as np
import numpy.testing as npt

import pandas as pd
import pandas.util.testing as pdt

import puget.cluster as cluster


def test_make_mapping():
    pass


def test_cluster_by_groups():
    # In the first case, all individuals are linked through a chain of
    # co-occurences:
    df1 = pd.DataFrame({'individual_var': [1, 2, 3, 4, 1, 2, 3, 4],
                        'group_var': [1, 1, 2, 2, 1, 2, 1, 2]})

    T = cluster.groups_co_occurence(df1, 'group_var', 'individual_var')
    true_T = np.array([[0, 1, 1, 0], [1, 0, 2, 1], [1, 2, 0, 1], [0, 1, 1, 0]])
    npt.assert_equal(T, true_T)

    df1_out = cluster.cluster(df1, 'group_var', 'individual_var')
    true_df1_out = pd.DataFrame({'individual_var': [1, 2, 3, 4, 1, 2, 3, 4],
                                 'group_var': [1, 1, 2, 2, 1, 2, 1, 2],
                                 'cluster': [1, 1, 1, 1, 1, 1, 1, 1]})

    pdt.assert_frame_equal(df1_out.sort_index(axis=1),
                           true_df1_out.sort_index(axis=1))



    # In the second case, individuals are unlinked into two clusters:
    df2 = pd.DataFrame({'individual_var': [1, 2, 3, 4, 1, 2, 3, 4],
                        'group_var': [1, 1, 3, 3, 1, 1, 3, 3]})

    T = cluster.groups_co_occurence(df2, 'group_var', 'individual_var')
    true_T = np.array([[0, 1, 0, 0], [1, 0, 0, 0], [0, 0, 0, 1], [0, 0, 1, 0]])
    T = cluster.groups_co_occurence(df2, 'group_var', 'individual_var')
    npt.assert_equal(T, true_T)

    # Cluster's are designated as [1, 2, 3, ...], even while the group variable
    # can have arbitrary values:
    df2_out = cluster.cluster(df2, 'group_var', 'individual_var')
    true_df2_out = pd.DataFrame({'individual_var': [1, 2, 3, 4, 1, 2, 3, 4],
                                 'group_var': [1, 1, 3, 3, 1, 1, 3, 3],
                                 'cluster': [1, 1, 2, 2, 1, 1, 2, 2]})

    pdt.assert_frame_equal(df2_out.sort_index(axis=1),
                           true_df2_out.sort_index(axis=1))
