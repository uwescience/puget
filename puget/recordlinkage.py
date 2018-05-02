"""

"""
import numpy as np
import recordlinkage as rl
import networkx


def block_and_match(df, block_variable, comparison_dict, match_threshold=2,
                    string_method="jarowinkler", string_threshold=0.85):
    """
    Use recordlinkage to block on one variable and compare on others

    """

    indexer = rl.BlockIndex(on=block_variable)
    pairs = indexer.index(df)
    compare = rl.Compare()
    for k, v in comparison_dict.items():
        if v == "string":
            compare.string(k, k, method=string_method,
                           threshold=string_threshold, label=k)
        if v == "date":
            compare.date(k, k, label=k)

    features = compare.compute(pairs, df)
    features["sum"] = features.sum(axis=1)
    features["match"] = features["sum"] > match_threshold

    return features


def link_records(prelink_ids, link_list):
    """
    Link records from a dataset, using an iterative approach

    Parameters
    ----------
    prelink_ids : DataFrame
        A Pandas DataFrame with prelinked data.

    link_list : list of dicts:
            [{'block_variable': 'lname',
              'match_variables':{"fname": "string",
                                  "ssn_as_str": "string",
                                  "dob":"date"}},
            {'block_variable': 'fname',
              'match_variables':{"lname": "string",
                                  "ssn_as_str": "string",
                                  "dob":"date"}},
            {'block_variable': 'ssn_as_str',
              'match_variables':{"fname": "string",
                                  "lname": "string",
                                  "dob":"date"}}]

    """
    matches = []
    for link in link_list:
        features = block_and_match(prelink_ids,
                                   link['block_variable'],
                                   link['match_variables'])
        matches.append(features[features["match"]])

    G = networkx.Graph()
    for match in matches:
        for ix, row in match.iterrows():
            G.add_edge(row.name[0], row.name[1])

    prelink_ids["linkage_PID"] = np.nan
    new_pid = 1
    for linked in networkx.connected.connected_components(G):
        prelink_ids.loc[linked, "linkage_PID"] = new_pid
        new_pid = new_pid + 1

    ix = prelink_ids["linkage_PID"].isnull()
    prelink_ids.loc[ix, "linkage_PID"] = np.arange(new_pid, new_pid + ix.sum())
    prelink_ids["linkage_PID"] = prelink_ids["linkage_PID"].astype(int)

    return prelink_ids