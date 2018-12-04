"""

"""
import numpy as np
import pandas as pd
import itertools
from scipy.sparse import csr_matrix
import networkx as nx


def make_mapping(unique_individuals):
    """
    Create a mapping between a set of unique indivdual id's and indices into
    co-occurrence and distance matrices.

    parameters
    ----------
    unique_individuals : array
        Contains the individual IDs of all the members of the data-frame

    Returns
    -------
    dict
        A mapping between the IDs and ordinals [0,..., n], where n is
        the number of unique IDs.
    """
    mapping = dict(zip(unique_individuals,
                       np.arange(unique_individuals.shape[0])))
    return mapping


def groups_co_occurrence(df, individual_var, group_var, T=None,
                         mapping=None, sparse=None):
    """
    Count the co-occurrence of individuals in a group.

    Parameters
    ----------
    df : DataFrame
        The data-frame with individual records to cluster.

    individual_var : string
        The variable (column) that identifies individuals.

    group_var: string
        The variable (column) that identifies groups. This is the clustering
        variable.

    T : ndarray, optional
        If provided, this is a matrix that defines the unweighted graph of
        connections between individuals. Default: None, which implies that
        a matrix of zeros is initialized.

    mapping : dict, optional
        If provided, defines a mapping between individual identifiers and
        indices in the T array. Default: None, which implies this dict
        is generated on the fly.

    sparse : bool, optional
        Whether to use a sparse CSR matrix to represent the graph.

    Returns
    -------
    Matrix with integer values that indicates the number of times individuals
    (mapped through mapping and inv_mapping) have appeared together in the
    same group.
    """
    unique_individuals = df[individual_var].unique()
    if T is None:
        if not sparse:
            T = np.zeros((unique_individuals.shape[0],
                          unique_individuals.shape[0]))
    if mapping is None:
        mapping = make_mapping(unique_individuals)
    else:
        mapping = mapping
    gb = df.groupby(group_var)

    if sparse:
        rows = []
        cols = []
        for gid, group in gb:
            ids = group[individual_var].unique()
            pairs = itertools.permutations(ids, 2)
            for pair in pairs:
                rows.append(mapping[pair[0]])
                cols.append(mapping[pair[1]])
        T = csr_matrix((np.ones(len(cols)), (rows, cols)))
    else:
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


def time_co_occurrence(df, individual_var, time_var, time_unit='ns',
                       time_delta=0, T=None, mapping=None):
    """
    Group by co-occurrence of the times of enrollment (entry, exit).

    Parameters
    ----------
    time_var : list
        A list of all the time-variables to consider for co-occurrence

    time_unit : string
        What unit is used to represent time? (default: 'ns')

    time_delta : float or int
        How many of the time-unit is still considered "co-occurrence"?
        (default: 0).
    """
    unique_individuals = df[individual_var].unique()
    if T is None:
        T = np.zeros((unique_individuals.shape[0],
                      unique_individuals.shape[0]))
    if mapping is None:
        mapping = make_mapping(unique_individuals)
    else:
        mapping = mapping

    # We'll identify differences as things smaller than this:
    dt0 = np.timedelta64(time_delta, time_unit)
    for tv in time_var:
        df[tv]
        # Broadcast and get pairwise time-differences:
        diff = np.array(df[tv])[:, None] - np.array(df[tv])[:, None].T
        # Anything larger than the time_delta would do here:
        diff[pd.isnull(diff)] = np.timedelta64(time_delta + 1, 'ns')
        idx = np.where(np.abs(diff) <= dt0)
        rows = [mapping[ii] for ii in df[individual_var][idx[0]]]
        cols = [mapping[ii] for ii in df[individual_var][idx[1]]]
        # Increment the co-occurence matrix where relevant:
        T[rows, cols] = T[rows, cols] + 1

    # Enforce self-to-self co-occurence of zero (consistent with group
    # clustering):
    np.fill_diagonal(T, 0)
    return T


def cluster(df, individual_var, group_var=None, time_var=None, time_unit='ns',
            time_delta=0, sparse=False):
    """
    Calculate clusters from a co-occurrence matrix

    Parameters
    ----------
    df : DataFrame
    individual_var : string
        A variable that identifies individuals
    group_var : string
        A variable to cluster on group co-occurrence
    time_var : string
        A variable to cluster on temporal co-occurrence
    time_unit : string
    time_delta : float or int
    sparse : bool, optional
        Whether to use a sparse CSR matrix to represent the graph. This may
        slow things down, but might be necessary for really large datasets.
    """
    unique_individuals = df[individual_var].unique()

    if sparse:
        T = None
    else:
        T = np.zeros((unique_individuals.shape[0],
                      unique_individuals.shape[0]))

    mapping = make_mapping(unique_individuals)

    if group_var is not None:
        T = groups_co_occurrence(df, individual_var, group_var, T=T,
                                 mapping=mapping, sparse=sparse)

    if time_var is not None:
        if sparse:
            raise NotImplementedError("""Can't use sparse matrices with
                                         time variable""")
        T = time_co_occurrence(df, individual_var, time_var,
                               time_unit=time_unit,
                               time_delta=time_delta,
                               T=T, mapping=mapping)

    clusters = {}
    if not sparse:
        T[np.tril_indices(T.shape[0])] = 0
        G = nx.Graph(T)
    else:
        G = nx.from_scipy_sparse_matrix(T)

    for i, c in enumerate(nx.connected_components(G)):
        for j in c:
            clusters[j] = i + 1

    df['cluster'] = df[individual_var].apply(lambda x: clusters[mapping[x]])
    return df
