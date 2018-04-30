
import numpy as np
import pandas as pd 
import recordlinkage as rl
from puget.utils import stringify_ssn
import networkx


def block_and_match(df, block_variable, comparison_dict, match_threshold=2, 
                    string_method="jarowinkler", string_threshold=0.85):
    """ 
    Use recordlinkage to block on one variable and compare on others 

    """
    
    indexer = rl.BlockIndex(on=block_variable)
    pairs = indexer.index(df)
    compare = rl.Compare()
    for k,v in comparison_dict: 
        if v == "string":
            compare.string(k, k, method=string_method, 
                           threshold=string_threshold, label=k)
        if v == "date": 
            compare.date(k, k, label=k)

    features = compare.compute(pairs, df)
    features["sum"] = features.sum(axis=1)
    features["match"] = features["sum"] > match_threshold

    return features

def link_records(prelink_ids):
    """ 
    Link records in the union dataset

    """
    features_lname = block_and_match(prelink_ids, 
                                     "lname", 
                                     {"fname": "string", 
                                      "ssn_as_str": "string", 
                                      "dob":"date"})

    features_fname = block_and_match(prelink_ids,
                                     "lname", 
                                     {"fname": "string", 
                                      "ssn_as_str": "string", 
                                      "dob":"date"})

    features_ssn = block_and_match(prelink_ids,
                                   "ssn_as_str", 
                                     {"fname": "string", 
                                      "lname": "string", 
                                      "dob":"date"})

    matches_lname = features_lname[features_lname["match"]]
    matches_fname = features_fname[features_fname["match"]]
    matches_ssn = features_ssn[features_ssn["match"]]

    G = networkx.Graph()
    for matches in [matches_lname, matches_fname, matches_ssn]: 
        for ix, row in matches.iterrows():
            G.add_edge(row.name[0], row.name[1])

    prelink_ids["linkage_PID"] = np.nan
    new_pid = 1
    for linked in networkx.connected.connected_components(G):
        prelink_ids.loc[linked, "linkage_PID"] = new_pid
        new_pid = new_pid + 1

    ix = prelink_ids["linkage_PID"].isnull() 
    prelink_ids.loc[ix, "linkage_PID"] = np.arange(new_pid, new_pid + ix.sum())