"""

"""
import numpy as np
import pandas as pd
import itertools
from collections import OrderedDict

try:
    from fastcluster import linkage
except ImportError:
    from scipy.cluster.hierarchy import linkage

from scipy.cluster.hierarchy import fcluster


def make_mapping(unique_individuals):
    """
    Create a mapping between a set of unique indivdual id's and indices into
    co-occurence and distance matrices.
    """
    mapping = dict(zip(unique_individuals,
                       np.arange(unique_individuals.shape[0])))

    return mapping

def groups_co_occurence(df, individual_var, group_var, T=None,
                        mapping=None):
    """
    Count the co-occurence of individuals in a group.

    Returns
    -------
    Matrix with integer values that indicates the number of times individuals
    (mapped through mapping and inv_mapping) have appeared together in the
    same group.
    """
    # Filter to non-null client_key and non-null group_key
    df = df[pd.notnull(df[individual_var])]
    df = df[pd.notnull(df[group_var])]

    unique_individuals = df[individual_var].unique()
    if T is None:
        T = np.zeros((unique_individuals.shape[0],
                      unique_individuals.shape[0]))
    if mapping is None:
        mapping = make_mapping(unique_individuals)
    else:
        mapping = mapping

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


def time_co_occurence(df, individual_var, time_var, T=None,
                      mapping=None):
    """
    Group by co-occurence of the times of enrollment.
    """
    raise NotImplementedError


def linkage_matrix(T, method='complete'):
    """
    Calculate a linkage matrix from a co-occurence matrix

    Parameters
    ----------
    T : co-occurence matrix
    """
    D = np.zeros_like(T)

    # Distances are the inverse of co-occurences:
    D[T > 0] = 1.0 / T[T > 0]
    # No co-occurence translates into a distance of 2 (which is equivalent to
    # infinity for our purposes):
    D[T == 0] = 2
    # Distance self-to-self is 0:
    np.fill_diagonal(D, 0)
    Z = linkage(D, method=method)
    return Z


def cluster(df, individual_var, group_var=None, time_var=None):
    """
    Calculate clusters from a co-occurence matrix

    Parameters
    ----------
    df : DataFrame
    individual_var : string
        A variable that identifies individuals
    group_var : string
        A variable to cluster on group co-occurence
    time_var : string
        A variable to cluster on temporal co-occurence
    """
    # Filter to non-null client_key and non-null group_key
    df = df[pd.notnull(df[individual_var])]

    if group_var is not None:
        df = df[pd.notnull(df[group_var])]

    unique_individuals = df[individual_var].unique()

    T = np.zeros((unique_individuals.shape[0],
                  unique_individuals.shape[0]))

    mapping = make_mapping(unique_individuals)

    if group_var is not None:
        T = groups_co_occurence(df, individual_var, group_var, T=T,
                                mapping=mapping)

    Z = linkage_matrix(T)
    clusters = fcluster(Z, t=1.01)

    df['cluster'] = df[individual_var].apply(lambda x: clusters[mapping[x]])
    return df
