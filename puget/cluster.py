

import numpy as np
import pandas as pd
import itertools
from collections import OrderedDict

try:
    from fastcluster import linkage
except ImportError:
    from scipy.cluster.hierarchy import linkage

import scipy.sparse as sps

def groups_distance(df, group_var, individual_var, T=None):
    """
    Calculate distances based a group
    """
    # Filter to non-null client_key and non-null group_key
    df = df[pd.notnull(df[individual_var])]
    df = df[pd.notnull(df[group_var])]

    unique_individuals = df[individual_var].unique()
    if T is None:
        T = sps.csr_matrix((unique_individuals.shape[0],
                            unique_individuals.shape[0]))

    # Make dictionaries mapping    matrix indices <--> ClientID's
    mapping = OrderedDict(zip(unique_individuals,
                          np.arange(unique_individuals.shape[0])))

    inv_mapping = OrderedDict((value, key)
                              for key, value in mapping.iteritems())

    gb = df.groupby(group_var)
    for gid, group in gb:
        ids = group[individual_var].unique()
        pairs = itertools.permutations(ids, 2)
        rows = []
        cols = []
        for pair in pairs:
            rows.append(mapping[pair[0]])
            cols.append(mapping[pair[1]])
        T[rows, cols] = T[rows, cols] + 1

    return T


def distance_matrix():
    """
    Calculate the full distance matrix.
    """
    pass
