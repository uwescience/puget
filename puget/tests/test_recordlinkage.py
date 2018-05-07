import numpy as np
import pandas as pd
import pandas.util.testing as pdt
from puget.recordlinkage import link_records

def test_linkage():
    link_list = [{'block_variable': 'lname',
                  'match_variables': {"fname": "string",
                                      "ssn_as_str": "string",
                                      "dob": "date"}},
                 {'block_variable': 'fname',
                  'match_variables': {"lname": "string",
                                      "ssn_as_str": "string",
                                      "dob": "date"}},
                 {'block_variable': 'ssn_as_str',
                  'match_variables': {"fname": "string",
                                      "lname": "string",
                                      "dob": "date"}}]

    # Simplest case - both items are identical in all respects:
    prelink_ids = pd.DataFrame(data = {'pid0':["PHA0_1", "HMIS0_1"],
                                       'ssn_as_str':['123456789', '123456789'],
                                       'lname':["QWERT", "QWERT"],
                                        'fname':["QWERT", "QWERT"],
                                        'dob':["1990-02-01", "1990-02-01"]})

    prelink_ids["dob"] = pd.to_datetime(prelink_ids["dob"])
    test_df = prelink_ids.copy()
    linked = link_records(prelink_ids, link_list)
    test_df["linkage_PID"] = [1, 1]
    pdt.assert_frame_equal(test_df, linked)

    # Items differ completely in their last name
    prelink_ids = pd.DataFrame(data={'pid0': ["PHA0_1", "HMIS0_1"],
                                     'ssn_as_str': ['123456789', '123456789'],
                                     'lname': ["ASDF", "QWERT"],
                                     'fname': ["QWERT", "QWERT"],
                                     'dob': ["1990-02-01", "1990-02-01"]})

    prelink_ids["dob"] = pd.to_datetime(prelink_ids["dob"])
    test_df = prelink_ids.copy()
    linked = link_records(prelink_ids, link_list)
    test_df["linkage_PID"] = [1, 1]
    pdt.assert_frame_equal(test_df, linked)


    # Items differ completely in their first name
    prelink_ids = pd.DataFrame(data={'pid0': ["PHA0_1", "HMIS0_1"],
                                     'ssn_as_str': ['123456789', '123456789'],
                                     'lname': ["QWERT", "QWERT"],
                                     'fname': ["ASDF", "QWERT"],
                                     'dob': ["1990-02-01", "1990-02-01"]})

    prelink_ids["dob"] = pd.to_datetime(prelink_ids["dob"])
    test_df = prelink_ids.copy()
    linked = link_records(prelink_ids, link_list)
    test_df["linkage_PID"] = [1, 1]
    pdt.assert_frame_equal(test_df, linked)

    # Items differ completely in their SSN
    prelink_ids = pd.DataFrame(data={'pid0': ["PHA0_1", "HMIS0_1"],
                                     'ssn_as_str': ['123456789', '246801357'],
                                     'lname': ["QWERT", "QWERT"],
                                     'fname': ["QWERT", "QWERT"],
                                     'dob': ["1990-02-01", "1990-02-01"]})

    prelink_ids["dob"] = pd.to_datetime(prelink_ids["dob"])
    test_df = prelink_ids.copy()
    linked = link_records(prelink_ids, link_list)
    test_df["linkage_PID"] = [1, 1]
    pdt.assert_frame_equal(test_df, linked)



    # Items differ by permutation in their last name
    prelink_ids = pd.DataFrame(data={'pid0': ["PHA0_1", "HMIS0_1"],
                                     'ssn_as_str': ['123456789', '123456789'],
                                     'lname': ["QERWT", "QWERT"],
                                     'fname': ["QWERT", "QWERT"],
                                     'dob': ["1990-02-01", "1990-02-01"]})

    prelink_ids["dob"] = pd.to_datetime(prelink_ids["dob"])
    test_df = prelink_ids.copy()
    linked = link_records(prelink_ids, link_list)
    test_df["linkage_PID"] = [1, 1]
    pdt.assert_frame_equal(test_df, linked)

    # Items differ by permutation in their first name
    prelink_ids = pd.DataFrame(data={'pid0': ["PHA0_1", "HMIS0_1"],
                                     'ssn_as_str': ['123456789', '123456789'],
                                     'lname': ["QWERT", "QWERT"],
                                     'fname': ["QWERT", "QEWRT"],
                                     'dob': ["1990-02-01", "1990-02-01"]})

    prelink_ids["dob"] = pd.to_datetime(prelink_ids["dob"])
    test_df = prelink_ids.copy()
    linked = link_records(prelink_ids, link_list)
    test_df["linkage_PID"] = [1, 1]
    pdt.assert_frame_equal(test_df, linked)


    # Items differ by permutation in their dob day/month
    prelink_ids = pd.DataFrame(data={'pid0': ["PHA0_1", "HMIS0_1"],
                                     'ssn_as_str': ['123456789', '123456789'],
                                     'lname': ["QWERT", "QWERT"],
                                     'fname': ["QWERT", "QEWRT"],
                                     'dob': ["1990-01-02", "1990-02-01"]})

    prelink_ids["dob"] = pd.to_datetime(prelink_ids["dob"])
    test_df = prelink_ids.copy()
    linked = link_records(prelink_ids, link_list)
    test_df["linkage_PID"] = [1, 1]
    pdt.assert_frame_equal(test_df, linked)

    # Items differ by a permutation in the last name + no ssn for one of them
    prelink_ids = pd.DataFrame(data={'pid0': ["PHA0_1", "HMIS0_1"],
                                     'ssn_as_str': ['123456789', np.nan],
                                     'lname': ["QWERT", "QEWRT"],
                                     'fname': ["QWERT", "QWERT"],
                                     'dob': ["1990-01-02", "1990-01-02"]})

    prelink_ids["dob"] = pd.to_datetime(prelink_ids["dob"])
    test_df = prelink_ids.copy()
    linked = link_records(prelink_ids, link_list)
    test_df["linkage_PID"] = [1, 1]
    pdt.assert_frame_equal(test_df, linked)


    # One item is not linked:
    prelink_ids = pd.DataFrame(data = {'pid0':["PHA0_1", "HMIS0_1", "HMIS0_2"],
                                       'ssn_as_str':['123456789', '123456789', "246801357"],
                                       'lname':["QWERT", "QWERT", "ASDF"],
                                        'fname':["QWERT", "QWERT", "ASDF"],
                                        'dob':["1990-02-01", "1990-02-01", "1977-03-04"]})

    prelink_ids["dob"] = pd.to_datetime(prelink_ids["dob"])
    test_df = prelink_ids.copy()
    linked = link_records(prelink_ids, link_list)
    test_df["linkage_PID"] = [1, 1, 2]
    pdt.assert_frame_equal(test_df, linked)



    # One item doesn't match, but has two potential linkages:
    prelink_ids = pd.DataFrame(data = {'pid0':["PHA0_1", "HMIS0_1", "HMIS0_2"],
                                       'ssn_as_str':['123456789', '123456789', "246801357"],
                                       'lname':["QWERT", "QWERT", "ASDF"],
                                        'fname':["QWERT", "QWERT", "QWERT"],
                                        'dob':["1990-02-01", "1990-02-01", "1990-02-01"]})

    prelink_ids["dob"] = pd.to_datetime(prelink_ids["dob"])
    linked = link_records(prelink_ids, link_list)
    test_df = prelink_ids.copy()
    test_df["linkage_PID"] = [1, 1, 2]
    pdt.assert_frame_equal(test_df, linked)

    # Reducing the threshold for matching, this gets matched nonetheless:
    prelink_ids = pd.DataFrame(data = {'pid0':["PHA0_1", "HMIS0_1", "HMIS0_2"],
                                       'ssn_as_str':['123456789', '123456789', "123456789"],
                                       'lname':["QWERT", "QWERT", "ASDF"],
                                        'fname':["QWERT", "QWERT", "QWERT"],
                                        'dob':["1990-02-01", "1990-02-01", "1990-02-01"]})

    prelink_ids["dob"] = pd.to_datetime(prelink_ids["dob"])
    linked = link_records(prelink_ids, link_list, match_threshold=0.1)
    test_df = prelink_ids.copy()
    test_df["linkage_PID"] = [1, 1, 1]
    pdt.assert_frame_equal(test_df, linked)
