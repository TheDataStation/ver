from os import listdir
from os.path import isfile, join
from collections import defaultdict
import pandas as pd
from pandas.util import hash_pandas_object
from DoD import material_view_analysis as mva

from tqdm import tqdm


def normalize(df):
    # df = df.astype(str).apply(lambda x: x.str.strip().str.lower())
    # infer and convert types (originally all columns have 'object' type)
    # print(df.infer_objects().dtypes)
    df = df.convert_dtypes(convert_string=False, convert_integer=False)
    # print(df.dtypes)
    df = df.apply(lambda x: x.astype(str).str.strip().str.lower() if (x.dtype == 'object') else x)
    # df = df.convert_dtypes()
    # print(df.columns.dtype)
    # print(df)
    return df


def get_dataframes(path):
    files = [path + f for f in listdir(path) if isfile(join(path, f)) and f != '.DS_Store']
    dfs = []
    for f in files:
        df = pd.read_csv(f, encoding='latin1', thousands=',')  # .replace('"','', regex=True)
        df = mva.curate_view(df)
        df = normalize(df)
        if len(df) > 0:  # only append valid df
            dfs.append((df, f))
    return dfs


def identify_compatible_groups(dataframes_with_metadata):
    already_classified = set()
    compatible_groups = []

    for t1, path1, md1 in dataframes_with_metadata:
        # these local variables are for this one view
        compatible_group = [path1]
        hashes1 = hash_pandas_object(t1, index=False)
        ht1 = hashes1.sum()
        if path1 in already_classified:
            continue
        for t2, path2, md2 in dataframes_with_metadata:
            if path1 == path2:  # same table
                continue
            # if t2 is in remove group
            if path2 in already_classified:
                continue
            hashes2 = hash_pandas_object(t2, index=False)
            ht2 = hashes2.sum()

            # are views compatible
            if ht1 == ht2:
                compatible_group.append(path2)
                already_classified.add(path1)
                already_classified.add(path2)
        # if len(compatible_group) > 1:
        #  cannot check this condition because now all views are analyzed from compatible groups
        compatible_groups.append(compatible_group)
    return compatible_groups


def summarize_views_and_find_candidate_complementary(dataframes_with_metadata):
    already_processed_complementary_pairs = set()

    contained_groups = []
    candidate_complementary_groups = []

    for df1, path1, md1 in dataframes_with_metadata:
        # these local variables are for this one view
        contained_group = [path1]

        hashes1_list = hash_pandas_object(df1, index=False)  # we only consider content
        hashes1_set = set(hashes1_list)
        for df2, path2, md2 in dataframes_with_metadata:
            if path1 == path2:  # same table
                continue
            hashes2_list = hash_pandas_object(df2, index=False)
            hashes2_set = set(hashes2_list)
            # are views potentially contained
            if len(hashes1_set) > len(hashes2_set):
                # is t2 contained in t1?
                if len(hashes2_set - hashes1_set) == 0:
                    contained_group.append(path2)
            else:
                if (path1 + "%%%" + path2) in already_processed_complementary_pairs \
                        or (path2 + "%%%" + path1) in already_processed_complementary_pairs:
                    continue  # already processed, skip computation
                # Verify that views are potentially complementary
                s12 = (hashes1_set - hashes2_set)
                s1_complement = set()
                if len(s12) > 0:
                    s1_complement.update((s12))
                s21 = (hashes2_set - hashes1_set)
                s2_complement = set()
                if len(s21) > 0:
                    s2_complement.update((s21))
                if len(s1_complement) > 0 and len(s2_complement) > 0:  # and, otherwise it's a containment rel
                    idx1 = [idx for idx, value in enumerate(hashes1_list) if value in s1_complement]
                    idx2 = [idx for idx, value in enumerate(hashes2_list) if value in s2_complement]
                    candidate_complementary_groups.append((df1, md1, path1, idx1, df2, md2, path2, idx2))
                    already_processed_complementary_pairs.add((path1 + "%%%" + path2))
                    already_processed_complementary_pairs.add((path2 + "%%%" + path1))
        if len(contained_group) > 1:
            contained_groups.append(contained_group)
    return contained_groups, candidate_complementary_groups


def pick_most_likely_key_of_pair(md1, md2):
    most_likely_key1 = sorted(md1.items(), key=lambda x: x[1], reverse=True)
    most_likely_key2 = sorted(md2.items(), key=lambda x: x[1], reverse=True)
    candidate_k1 = most_likely_key1[0]
    candidate_k2 = most_likely_key2[0]
    # pick only one key so we make sure the group-by is compatible
    if candidate_k1 >= candidate_k2:
        k = candidate_k1[0]
    else:
        k = candidate_k2[0]
    return k


def find_candidate_keys(df, sampling=True, max_num_attr_in_composite_key=4):
    candidate_keys = []

    # infer and convert types (originally all columns have 'object' type)
    # print(df.infer_objects().dtypes)
    df = df.convert_dtypes()
    # df = mva.curate_view(df)
    # for col in df.columns:
    #     try:
    #         print(df[col])
    #         df[col] = pd.to_numeric(df[col])
    #     except:
    #         pass
    # print(df.dtypes)

    # drop float-type columns
    df = df.select_dtypes(exclude=['float'])
    # print(df.dtypes)
    total_rows, total_cols = df.shape

    sample = df
    sample_size = total_rows

    if total_cols == 0:
        return candidate_keys

    if sampling:
        # a minimum sample size of O ( (T^½) (ε^-1) (d + log (δ^−1) ) is needed to ensure that, with probability (1−δ),
        # the strength of each key discovered in a sample exceeds 1−ε. T is the number of entities and d is the
        # number of attributes.
        delta_prob = 0.99
        epsilon = 0.99
        import math
        sample_size = math.ceil(math.sqrt(total_rows) * 1 / epsilon * (total_cols + math.log(1 / delta_prob)))
        if sample_size < total_rows:
            sample = df.sample(n=sample_size, replace=False)

    from itertools import chain, combinations

    def power_set(iterable, size_range):
        "power_set([1,2,3], (0, 3)) --> () (1,) (2,) (3,) (1,2) (1,3) (2,3) (1,2,3)"
        s = list(iterable)
        return chain.from_iterable(combinations(s, r) for r in range(size_range[0], size_range[1] + 1))

    # print(sample.dtypes)
    possible_keys = power_set(sample.columns.values, (1, max_num_attr_in_composite_key))

    # TODO: pruning
    # Find all candidate keys that have the same/similar maximum strength
    max_strength = 0
    epsilon = 10e-3
    for key in map(list, filter(None, possible_keys)):
        # strength of a key = the number of distinct key values in the dataset divided by the number of rows
        num_groups = sample.groupby(key).ngroups
        strength = num_groups / sample_size

        if abs(strength - max_strength) < epsilon:
            candidate_keys.append(tuple(key))
        elif strength > max_strength:
            candidate_keys = [tuple(key)]
            max_strength = strength

    # print("candidate keys:", candidate_keys)

    return candidate_keys


def transform_to_lowercase(df):
    df = df.astype(str).apply(lambda x: x.str.strip().str.lower())
    return df


def find_complementary_or_contradictory_keys(t1, idx1, t2, idx2, candidate_key_size):
    result = {}

    # Find all candidate keys for both views (before selecting specific idx for comparison)
    candidate_keys_1 = find_candidate_keys(t1, sampling=False, max_num_attr_in_composite_key=candidate_key_size)
    candidate_keys_2 = find_candidate_keys(t2, sampling=False, max_num_attr_in_composite_key=candidate_key_size)
    # print(candidate_keys_1)
    # print(candidate_keys_2)

    # if we didn't find any key (ex. all the columns are float), then we don't classify any
    # contradictory or complementary groups
    if len(candidate_keys_1) == 0 and len(candidate_keys_2) == 0:
        return result

    candidate_keys = set(candidate_keys_1).intersection(set(candidate_keys_2))
    # print(candidate_keys)

    selection1 = t1.iloc[idx1]
    selection2 = t2.iloc[idx2]

    for candidate_key_tuple in candidate_keys:

        candidate_key = list(candidate_key_tuple)

        complementary_keys1 = set()
        complementary_keys2 = set()
        contradictory_keys = set()
        # contradictory_keys2 = set()

        # Since selection1 and selection2 has the same set of attributes, if their intersection on the composite key
        # contains overlapping columns that are contradictory (ex. Room Square  Footage_left = 1000 and Room Square
        # Footage_right = 1001), then its corresponding key value is a contradictory key. Any key values not
        # contained in
        # the intersection are complementary keys.

        # print(candidate_key)
        # print("selection1:", selection1[candidate_key].dtypes)
        # print("selection2:", selection2[candidate_key].dtypes)
        # There are rare cases when the dtypes of the candidate key don't match (even though they have the same
        # attributes),
        # because convert_dtypes() may convert the same attribute to int in one df (if it only has one row of 0, for ex)
        # and float in another df. In this case the candidate key is obviously invalid so it's okay to skip it
        type_match = True
        for k in candidate_key:
            if selection1[k].dtype != selection2[k].dtype:
                type_match = False
        if not type_match:
            continue

        # Finding contradictory keys using inner join
        inner_merge = pd.merge(selection1, selection2, on=candidate_key, how='left', suffixes=('_left', '_right'))
        # this is the intersection of the composite keys from both selections
        inner_merge_intersection = inner_merge[inner_merge.notnull().all(axis=1)]
        if not inner_merge_intersection.empty:
            for c in selection1.columns.values:
                if c not in candidate_key:
                    # compare two columns with the same attribute (now with suffix '_left' or '_right')
                    # and get the key values of all the rows that have contradictions
                    contradictory_keys_df = inner_merge_intersection.loc[
                        ~(inner_merge_intersection[c + '_left'] == inner_merge_intersection[c + '_right'])][
                        candidate_key]
                    # need to convert to a set of tuples, each tuple is the contradicting key values (one or multiple
                    # columns)
                    curr_contradictory_keys = set(tuple(row) for row in contradictory_keys_df.values.tolist())
                    if len(curr_contradictory_keys) > 0:
                        # TODO: do I keep only one contradiction key values or all?
                        # Find all contradictions for now
                        contradictory_keys.update(curr_contradictory_keys)
                        # contradictory_keys1.add(contradictory_keys.pop())
                        # no need to continue once we found a contradiction for some attribute
                        # break

        if len(contradictory_keys) == 0:
            if len(selection1.columns) == len(candidate_key):
                # add dummy column
                selection1["dummy_col"] = ""
                selection2["dummy_col"] = ""

            # Finding complementary keys using from view 1 to view 2 and view 2 to view 1
            left_merge = pd.merge(selection1, selection2, on=candidate_key, how='left', suffixes=('_left', '_right'))
            # this is the intersection of the composite keys from both selections
            left_merge_intersection = left_merge[left_merge.notnull().all(axis=1)]
            # this is all the rows that are not contained in the intersection (so they have NaNs)
            complementary_keys_df = left_merge[left_merge.isnull().any(axis=1)][candidate_key]
            complementary_keys = set(tuple(row) for row in complementary_keys_df.values.tolist())
            complementary_keys1.update(complementary_keys)

            # mirror of above
            right_merge = pd.merge(selection1, selection2, on=candidate_key, how='right', suffixes=('_left', '_right'))
            right_merge_intersection = right_merge[right_merge.notnull().all(axis=1)]
            complementary_keys_df = right_merge[right_merge.isnull().any(axis=1)][candidate_key]
            complementary_keys = set(tuple(row) for row in complementary_keys_df.values.tolist())
            complementary_keys2.update(complementary_keys)

        # print(complementary_key1, complementary_key2, contradictory_key1, contradictory_key2)
        result[candidate_key_tuple] = [complementary_keys1, complementary_keys2, contradictory_keys]

    return result

    # s1 = selection1[k]
    # s2 = set(selection2[k])
    # for key in tqdm(s1):
    #     if len(contradictory_key1) > 0:  # check this condition for early skip
    #         break
    #     if key in s2:
    #         for c in selection1.columns:
    #             cell_value1 = set(selection1[selection1[k] == key][c])
    #             cell_value2 = set(selection2[selection2[k] == key][c])
    #             if len(cell_value1 - cell_value2) != 0:
    #                 contradictory_key1.add(key)
    #                 break  # one contradictory example is sufficient
    #     else:
    #         complementary_key1.add(key)
    # if len(contradictory_key1) == 0:  # if we found a contradictory example, no need to go on
    #     s2 = set(selection2[k]) - set(s1)  # we only check the set difference to save some lookups
    #     s1 = set(selection1[k])
    #     for key in tqdm(s2):
    #         if len(contradictory_key2) > 0:  # check this condition for early skip
    #             break
    #         if key in s1:
    #             for c in selection2.columns:
    #                 cell_value2 = set(selection2[selection2[k] == key][c])
    #                 cell_value1 = set(selection1[selection1[k] == key][c])
    #                 if len(cell_value2 - cell_value1) != 0:
    #                     contradictory_key2.add(key)
    #                     break
    #         else:
    #             complementary_key2.add(key)


# def tell_contradictory_and_complementary_chasing(candidate_complementary_group, t_to_remove):
#     complementary_group = list()
#     contradictory_group = list()
#
#     contradictory_pairs = set()
#     complementary_pairs = set()
#
#     graph = defaultdict(dict)
#
#     # create undirected graph
#     for df1, md1, path1, idx1, df2, md2, path2, idx2 in tqdm(candidate_complementary_group):
#         # if the view is gonna be summarized, then there's no need to check this one either. Not in graph
#         if path1 in t_to_remove or path2 in t_to_remove:
#             continue  # this will be removed, no need to worry about them
#         graph[path1][path2] = (df1, md1, path1, idx1, df2, md2, path2, idx2)
#         graph[path2][path1] = (df1, md1, path1, idx1, df2, md2, path2, idx2)
#
#     there_are_unexplored_pairs = True
#
#     marked_nodes = set()
#
#     while there_are_unexplored_pairs:
#
#         while len(marked_nodes) > 0:
#
#             marked_node = marked_nodes.pop()
#             # print(marked_node)
#             path, composite_key_tuple, contradictory_key_tuple = marked_node  # pop on insertion
#             # contradictory_key = contradictory_keys.pop()
#             composite_key = list(composite_key_tuple)
#             # there is only one (tuple) contradictory key here
#             contradictory_key = list(contradictory_key_tuple)
#
#             neighbors_graph = graph[path]
#             # chase all neighbors of involved node
#             for neighbor_k, neighbor_v in neighbors_graph.items():
#                 df1, md1, path1, idx1, df2, md2, path2, idx2 = neighbor_v
#                 # skip already processed pairs
#                 if path1 + "%$%" + path2 in contradictory_pairs or path2 + "%$%" + path1 in contradictory_pairs \
#                         or path1 + "%$%" + path2 in complementary_pairs or path2 + "%$%" + path1 in
#                         complementary_pairs:
#                     continue
#                 selection1 = df1.iloc[idx1]
#                 selection2 = df2.iloc[idx2]
#
#                 for c in selection1.columns:
#
#                     # select row based on multiple composite keys
#                     condition1 = (selection1[composite_key[0]] == contradictory_key[0])
#                     condition2 = (selection2[composite_key[0]] == contradictory_key[0])
#                     for i in range(1, len(composite_key)):
#                         condition1 = (condition1 & (selection1[composite_key[i]] == contradictory_key[i]))
#                         condition2 = (condition2 & (selection2[composite_key[i]] == contradictory_key[i]))
#
#                     cell_value1 = set(selection1.loc[condition1][c])
#                     cell_value2 = set(selection2.loc[condition2][c])
#
#                     if len(cell_value1) > 0 and len(cell_value2) > 0 and len(cell_value1 - cell_value2) != 0:
#                         # {contradictory_key_tuple} need to be a set!!!
#                         contradictory_group.append((path1, composite_key_tuple, {contradictory_key_tuple}, path2))
#                         contradictory_pairs.add(path1 + "%$%" + path2)
#                         contradictory_pairs.add(path2 + "%$%" + path1)
#                         marked_nodes.add((neighbor_k, composite_key_tuple, contradictory_key_tuple))
#                         break  # one contradiction is enough to move on, no need to check other columns
#
#         # At this point all marked nodes are processed. If there are no more candidate pairs, then we're done
#         if len(candidate_complementary_group) == 0:
#             there_are_unexplored_pairs = False
#             break
#
#         # TODO: pick any pair (later refine hwo to choose this, e.g., pick small cardinality one)
#         df1, md1, path1, idx1, df2, md2, path2, idx2 = candidate_complementary_group.pop()  # random pair
#         # check we havent processed this pair yet -- we may have done it while chasing from marked_nodes
#         if path1 + "%$%" + path2 in contradictory_pairs or path2 + "%$%" + path1 in contradictory_pairs \
#                 or path1 + "%$%" + path2 in complementary_pairs or path2 + "%$%" + path1 in complementary_pairs:
#             continue
#
#         # find contradiction in pair (if not put in complementary group and choose next pair)
#         # k = pick_most_likely_key_of_pair(md1, md2)
#
#         # switching to use best_composite_key instead of finding a key for every pair of views
#         dfs = [df1, df2]
#         best_composite_key = find_composite_key(dfs, sampling=False, max_num_attr_in_composite_key=4)
#         # if we didn't find a composite key (ex. all the columns are float), then we don't classify any
#         # contradictory or complementary groups
#         if best_composite_key == None:
#             return complementary_group, contradictory_group
#
#         complementary_key1, complementary_key2, \
#         contradictory_key1, contradictory_key2 = find_contradiction_pair(df1, idx1, df2, idx2, best_composite_key)
#
#         # print(contradictory_key1, contradictory_key2)
#
#         composite_key_tuple = tuple(best_composite_key)
#
#         # if contradiction found, mark keys and nodes of graph
#         if len(contradictory_key1) > 0:
#             # tuple is: (path1: name of table, composite_key_tuple, contr_key1 (one tuple))
#             contr_key1 = contradictory_key1.pop()
#             marked_nodes.add((path1, composite_key_tuple, contr_key1))
#         if len(contradictory_key2) > 0:
#             contr_key2 = contradictory_key2.pop()
#             marked_nodes.add((path2, composite_key_tuple, contr_key2))
#
#         # record the classification between complementary/contradictory of this iteration
#         if len(contradictory_key1) > 0:
#             # tuple is: (path1, composite_key_tuple, contradictory_key: set of contradictory keys (tuples), path2)
#             contradictory_group.append((path1, composite_key_tuple, contradictory_key1, path2))
#             # print((path1, composite_key_tuple, contradictory_key1, path2))
#             contradictory_pairs.add(path1 + "%$%" + path2)
#             contradictory_pairs.add(path2 + "%$%" + path1)
#         if len(contradictory_key2) > 0:
#             contradictory_group.append((path2, composite_key_tuple, contradictory_key2, path1))
#             # print((path2, composite_key_tuple, contradictory_key2, path1))
#             contradictory_pairs.add(path1 + "%$%" + path2)
#             contradictory_pairs.add(path2 + "%$%" + path1)
#         if len(contradictory_key1) == 0 and len(contradictory_key2) == 0:
#             if path1 + "%$%" + path2 in contradictory_pairs or path2 + "%$%" + path1 in contradictory_pairs:
#                 continue
#             else:
#                 complementary_group.append((path1, path2, complementary_key1, complementary_key2))
#                 complementary_pairs.add(path1 + "%$%" + path2)
#                 complementary_pairs.add(path2 + "%$%" + path1)
#
#     return complementary_group, contradictory_group
#
#
# def chasing_4c(dataframes_with_metadata):
#     # sort relations by cardinality to avoid reverse containment
#     # (df, path, metadata)
#     dataframes_with_metadata = sorted(dataframes_with_metadata, key=lambda x: len(x[0]), reverse=True)
#
#     compatible_groups = identify_compatible_groups(dataframes_with_metadata)
#     # We pick one representative from each compatible group
#     selection = set([x[0] for x in compatible_groups])
#     dataframes_with_metadata_selected = [(df, path, metadata) for df, path, metadata in dataframes_with_metadata
#                                          if path in selection]
#
#     contained_groups, candidate_complementary_group = \
#         summarize_views_and_find_candidate_complementary(dataframes_with_metadata_selected)
#
#     # complementary_group, contradictory_group = \
#     #     tell_contradictory_and_complementary_chasing(candidate_complementary_group, t_to_remove)
#     t_to_remove = set()
#
#     complementary_group, contradictory_group = \
#         tell_contradictory_and_complementary_chasing(candidate_complementary_group, t_to_remove)
#
#     # prepare found groups for presentation
#     compatible_groups = [cg for cg in compatible_groups if len(cg) > 1]
#
#     return compatible_groups, contained_groups, complementary_group, contradictory_group


def tell_contradictory_and_complementary_allpairs(candidate_complementary_group, t_to_remove, candidate_key_size):
    complementary_group = list()
    contradictory_group = list()

    # contradictory_pairs = set()
    # complementary_pairs = set()

    all_pair_result = {}

    for t1, md1, path1, idx1, t2, md2, path2, idx2 in tqdm(candidate_complementary_group):
        if path1 in t_to_remove or path2 in t_to_remove:
            continue  # this will be removed, no need to worry about them

        # For each candidate key in each view, find whether the views have contradictions or are complementary.
        # Returns a dictionary {candidate_key1:[complementary_keys1, complementary_keys2, contradictory_keys]
        #                       candidate_key2:[...], ...}
        # If the views have contradictions, complementary_keys will be empty and contradictory_keys will have value,
        # if the views are complementary, the opposite.
        result = find_complementary_or_contradictory_keys(t1, idx1, t2, idx2, candidate_key_size)

        all_pair_result[(path1, path2)] = result

        for candidate_key_tuple, result_list in result.items():

            complementary_keys1 = result_list[0]
            complementary_keys2 = result_list[1]
            contradictory_keys = result_list[2]

            # record the classification between complementary/contradictory of this iteration
            if len(contradictory_keys) > 0:
                # tuple is: (path1, composite_key_tuple, contradictory_key: set of contradictory keys (tuples), path2)
                contradictory_group.append((path1, candidate_key_tuple, contradictory_keys, path2))
            if len(contradictory_keys) == 0:
                complementary_group.append(
                    (path1, path2, candidate_key_tuple, complementary_keys1, complementary_keys2))

    return complementary_group, contradictory_group, all_pair_result


def no_chasing_4c(dataframes_with_metadata, candidate_key_size):
    # sort relations by cardinality to avoid reverse containment
    # (df, path, metadata)
    dataframes_with_metadata = sorted(dataframes_with_metadata, key=lambda x: len(x[0]), reverse=True)

    compatible_groups = identify_compatible_groups(dataframes_with_metadata)
    # We pick one representative from each compatible group
    selection = set([x[0] for x in compatible_groups])
    dataframes_with_metadata_selected = [(df, path, metadata) for df, path, metadata in dataframes_with_metadata
                                         if path in selection]

    contained_groups, candidate_complementary_group = \
        summarize_views_and_find_candidate_complementary(dataframes_with_metadata_selected)

    t_to_remove = set()
    complementary_group, contradictory_group, all_pair_contr_compl = \
        tell_contradictory_and_complementary_allpairs(candidate_complementary_group, t_to_remove, candidate_key_size)

    # complementary_group, contradictory_group = \
    #     tell_contradictory_and_complementary_chasing(candidate_complementary_group, t_to_remove)

    # prepare found groups for presentation
    compatible_groups = [cg for cg in compatible_groups if len(cg) > 1]

    return compatible_groups, contained_groups, complementary_group, contradictory_group, all_pair_contr_compl


def brute_force_4c_valuewise(dataframes_with_metadata):
    def are_views_contradictory(t1, t2, md1, md2):
        mlk1 = sorted(md1.items(), key=lambda x: x[1], reverse=True)
        mlk2 = sorted(md2.items(), key=lambda x: x[1], reverse=True)
        candidate_k1 = mlk1[0]
        candidate_k2 = mlk2[0]
        # pick only one key so we make sure the group-by is compatible
        if candidate_k1 >= candidate_k2:
            k = candidate_k1
        else:
            k = candidate_k2
        vg1 = t1.groupby([k[0]])
        vg2 = t2.groupby([k[0]])
        if len(vg1.groups) > len(vg2.groups):
            vref = vg1
            voth = vg2
        else:
            vref = vg2
            voth = vg1
        contradictions = []
        for gn, gv in vref:
            if gn not in voth:  # this cannot be accounted for contradiction -> could be complementary
                continue
            v = voth.get_group(gn)
            are_equivalent, equivalency_type = mva.equivalent(gv, v)  # local group
            if not are_equivalent:
                contradictions.append((k, gn))
                break  # one contradiction is enough to stop
        if len(contradictions) != 0:
            return True, contradictions
        return False, []

    def are_they_complementary(t1, t2, md1, md2):
        mlk1 = sorted(md1.items(), key=lambda x: x[1], reverse=True)
        mlk2 = sorted(md2.items(), key=lambda x: x[1], reverse=True)
        k1 = mlk1[0]
        k2 = mlk2[0]
        s1 = set(t1[k1[0]])
        s2 = set(t2[k2[0]])
        s12 = (s1 - s2)
        sdiff = set()
        if len(s12) > 0:
            sdiff.update((s12))
        s21 = (s2 - s1)
        if len(s21) > 0:
            sdiff.update((s21))
        if len(sdiff) == 0:
            return False, set([])
        return True, sdiff

    summarized_group = list()
    complementary_group = list()
    contradictory_group = list()

    t_to_remove = set()

    for t1, path1, md1 in dataframes_with_metadata:
        # let's first detect whether this view is going to be removed
        ht1 = hash_pandas_object(t1).sum()
        if ht1 in t_to_remove:
            continue
        for t2, path2, md2 in dataframes_with_metadata:
            if path1 == path2:  # same table
                continue
            ht2 = hash_pandas_object(t2).sum()
            # if t2 is in remove group
            if path2 in t_to_remove:
                continue
            # Are they compatible
            if ht1 == ht2:
                t_to_remove.add(path2)

            # t2 smaller -> it may be contained in t1
            elif len(t1) > len(t2):
                is_contained = True
                for c in t1.columns:
                    small_set = t2[c].apply(lambda x: str(x).lower())
                    large_set = t1[c].apply(lambda x: str(x).lower())
                    dif = set(small_set) - set(large_set)
                    if len(dif) != 0:
                        is_contained = False
                        break
                if is_contained:
                    t_to_remove.add(path2)
            else:
                # Detect if views are contradictory...
                contradictory, contradictions = are_views_contradictory(t1, t2, md1, md2)
                if contradictory:
                    contradictory_group.append((path1, path2, contradictions))
                else:
                    # not contradictory, are they complementary?
                    complementary, complement = are_they_complementary(t1, t2, md1, md2)
                    if complementary:
                        complementary_group.append((path1, path2, complement))

    # summarize out contained and compatible views
    for t, path, md in dataframes_with_metadata:
        if path not in t_to_remove:
            summarized_group.append(path)
    return summarized_group, complementary_group, contradictory_group


def classify_per_table_schema(dataframes):
    """
    Two schemas are the same if they have the exact same number and type of columns
    :param dataframes:
    :return:
    """
    schema_id_info = dict()
    schema_to_dataframes = defaultdict(list)
    for df, path in dataframes:
        the_hashes = [hash(el) for el in df.columns]
        schema_id = sum(the_hashes)
        schema_id_info[schema_id] = len(the_hashes)
        schema_to_dataframes[schema_id].append((df, path))
    return schema_to_dataframes, schema_id_info


def get_df_metadata(dfs):
    dfs_with_metadata = []
    for df, path in dfs:
        metadata = mva.uniqueness(df)
        dfs_with_metadata.append((df, path, metadata))
    return dfs_with_metadata


def main(input_path, candidate_key_size):
    # groups_per_column_cardinality = defaultdict(dict)
    compatible_groups = []
    contained_groups = []
    complementary_groups = []
    contradictory_groups = []
    all_pair_contr_compl = {}

    dfs = get_dataframes(input_path)
    print("Found " + str(len(dfs)) + " valid tables")

    dfs_per_schema, schema_id_info = classify_per_table_schema(dfs)
    print("View candidates classify into " + str(len(dfs_per_schema)) + " groups based on schema")
    print("")
    for key, group_dfs in dfs_per_schema.items():
        print("Num elements with schema " + str(key) + " is: " + str(len(group_dfs)))

        # best_composite_key = find_composite_key(group_dfs)

        # TODO: the metadata is not needed anymore, but keep it for now so I don't ruin the code structure
        dfs_with_metadata = get_df_metadata(group_dfs)

        # summarized_group, complementary_group, contradictory_group = brute_force_4c(dfs_with_metadata)
        compatible_group, contained_group, complementary_group, contradictory_group, all_pair_contr_compl_cur = \
            no_chasing_4c(
                dfs_with_metadata, candidate_key_size)

        compatible_groups += compatible_group
        contained_groups += contained_group
        complementary_groups += complementary_group
        contradictory_groups += contradictory_group
        all_pair_contr_compl.update(all_pair_contr_compl_cur)

        # groups_per_column_cardinality[key]['compatible'] = compatible_group
        # groups_per_column_cardinality[key]['contained'] = contained_group
        # groups_per_column_cardinality[key]['complementary'] = complementary_group
        # groups_per_column_cardinality[key]['contradictory'] = contradictory_group
        # groups_per_column_cardinality[key]['all_pair_contr_compl'] = all_pair_contr_compl

    return compatible_groups, contained_groups, complementary_groups, contradictory_groups, all_pair_contr_compl


def nochasing_main(input_path):
    groups_per_column_cardinality = defaultdict(dict)

    dfs = get_dataframes(input_path)
    print("Found " + str(len(dfs)) + " valid tables")

    dfs_per_schema, schema_id_info = classify_per_table_schema(dfs)
    print("View candidates classify into " + str(len(dfs_per_schema)) + " groups based on schema")
    print("")
    for key, group_dfs in dfs_per_schema.items():
        print("Num elements with schema " + str(key) + " is: " + str(len(group_dfs)))
        dfs_with_metadata = get_df_metadata(group_dfs)

        # summarized_group, complementary_group, contradictory_group = brute_force_4c(dfs_with_metadata)
        compatible_group, contained_group, complementary_group, contradictory_group, all_pair_contr_compl = \
            no_chasing_4c(
                dfs_with_metadata)
        groups_per_column_cardinality[key]['compatible'] = compatible_group
        groups_per_column_cardinality[key]['contained'] = contained_group
        groups_per_column_cardinality[key]['complementary'] = complementary_group
        groups_per_column_cardinality[key]['contradictory'] = contradictory_group
        groups_per_column_cardinality[key]['all_pair_contr_compl'] = all_pair_contr_compl

    return groups_per_column_cardinality


def valuewise_main(input_path):
    groups_per_column_cardinality = defaultdict(dict)

    dfs = get_dataframes(input_path)
    print("Found " + str(len(dfs)) + " valid tables")

    dfs_per_schema = classify_per_table_schema(dfs)
    print("View candidates classify into " + str(len(dfs_per_schema)) + " groups based on schema")
    print("")
    for key, group_dfs in dfs_per_schema.items():
        print("Num elements with schema " + str(key) + " is: " + str(len(group_dfs)))
        dfs_with_metadata = get_df_metadata(group_dfs)

        # summarized_group, complementary_group, contradictory_group = brute_force_4c(dfs_with_metadata)
        summarized_group, complementary_group, contradictory_group = brute_force_4c_valuewise(dfs_with_metadata)
        # groups_per_column_cardinality[key]['compatible'] = compatible_group
        # groups_per_column_cardinality[key]['contained'] = contained_group
        # groups_per_column_cardinality[key]['complementary'] = complementary_group
        # groups_per_column_cardinality[key]['contradictory'] = contradictory_group

    return groups_per_column_cardinality


if __name__ == "__main__":
    print("View 4C Analysis - Baseline")

    pd.set_option('display.max_columns', None)
    # pd.set_option('display.max_rows', None)
    pd.set_option('display.max_colwidth', None)
    pd.set_option('display.width', None)  # or 199

    # SNIPPET of CODE TO FIGURE OUT HASH ERROR BECAUSE USE OF INDEX
    # import pandas as pd
    #
    # df7 = pd.read_csv("/Users/ra-mit/development/discovery_proto/DoD/dod_evaluation/vassembly/many/qv2/view_7",
    #                   encoding='latin1')
    # df21 = pd.read_csv("/Users/ra-mit/development/discovery_proto/DoD/dod_evaluation/vassembly/many/qv2/view_21",
    #                    encoding='latin1')
    # dfs_with_metadata = get_df_metadata([(df7, 'path7'), (df21, 'path21')])
    # compatible_groups = identify_compatible_groups(dfs_with_metadata)
    # contained_groups, candidate_complementary_group = \
    #     summarize_views_and_find_candidate_complementary(dfs_with_metadata)
    # exit()

    # input_path = "/Users/ra-mit/development/discovery_proto/data/dod/mitdwh/two/"
    input_path = "./toytest/"

    compatible_group, contained_group, complementary_group, contradictory_group, all_pair_contr_compl = \
        main(input_path, candidate_key_size=2)

    print("Compatible groups: " + str(len(compatible_group)))
    print("Contained groups: " + str(len(contained_group)))
    print("Complementary views: " + str(len(complementary_group)))
    print("Contradictory views: " + str(len(contradictory_group)))

    print("Compatible groups:")
    for group in compatible_group:
        print(group)

    print("Contained groups:")
    for group in contained_group:
        print(group)

    print("Complementary views: ")
    for path1, path2, _, _, _ in complementary_group:
        print(path1 + " - " + path2)

    print("Contradictory views: ")
    for path1, candidate_key_tuple, key_value_tuples, path2 in contradictory_group:

        df1 = pd.read_csv(path1)
        df1 = normalize(df1)
        df1 = df1.sort_index(axis=1)
        df2 = pd.read_csv(path2)
        df2 = normalize(df2)
        df2 = df2.sort_index(axis=1)

        # a list containing candidate key names
        candidate_key = list(candidate_key_tuple)
        # a list of tuples, each tuple corresponds to the contradictory key values
        key_values = list(key_value_tuples)

        print("Candidate key: ", candidate_key)

        # there could be multiple contradictions existing in a pair of views
        assert len(key_values) >= 1
        for key_value in key_values:

            # select row based on multiple composite keys
            assert len(key_value) == len(candidate_key)

            condition1 = (df1[candidate_key[0]] == key_value[0])
            condition2 = (df2[candidate_key[0]] == key_value[0])
            for i in range(1, len(candidate_key)):
                condition1 = (condition1 & (df1[candidate_key[i]] == key_value[i]))
                condition2 = (condition2 & (df2[candidate_key[i]] == key_value[i]))

            row1 = df1.loc[condition1]
            row2 = df2.loc[condition2]

            print(path1 + " - " + path2)
            print("ROW - 1")
            print(row1)
            print("ROW - 2")
            print(row2)
            print("")

        print("")

    # analyzing contradictory views:
    mapping = defaultdict(list)
    for path1, _, _, path2 in contradictory_group:
        mapping[path1].append(path2)
        mapping[path2].append(path1)
    for k, v in mapping.items():
        print(k + " : " + str(len(v)))
