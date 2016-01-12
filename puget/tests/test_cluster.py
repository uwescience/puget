
import numpy as np
import numpy.testing as npt

import pandas as pd
import pandas.util.testing as pdt

import puget.cluster as cluster

def test_groups_distance():

    df1 = pd.DataFrame({'individual_var': [1, 2, 3, 4, 1, 2, 3, 4],
                             'group_var': [1, 1, 2, 2, 1, 2, 1, 2]})

    T = cluster.groups_distance(df1, 'group_var', 'individual_var')
    true_T = np.array([[0, 1, 1, 0], [1, 0, 2, 1], [1, 2, 0, 1], [0, 1, 1, 0]])
    npt.assert_equal(T.toarray(), true_T)
