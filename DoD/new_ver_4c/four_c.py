import itertools
import pprint
import time
from os import listdir
from os.path import isfile, join
from pandas.util import hash_pandas_object

from itertools import chain, combinations
from utils import *


def get_dataframes(path, view_paths):
    if view_paths is not None:
        files = view_paths
    else:
        files = [path + f for f in listdir(path) if isfile(join(path, f)) and f != '.DS_Store' and f != "log.txt"]
    dfs = []
    path_to_df_dict = {}
    total_num_rows = 0
    for f in files:
        df = pd.read_csv(f, encoding='latin1', thousands=',')  # .replace('"','', regex=True)
        df = curate_view(df)
        df = normalize(df)
        if len(df) > 0:  # only append valid df
            dfs.append((df, f))
            total_num_rows += len(df)
        path_to_df_dict[f] = df
    return dfs, path_to_df_dict, total_num_rows


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


def identify_compatible_contained_views_optimized(dfs):
    # already_classified_as_compatible = set()
    already_classified_as_contained = set()
    # already_classified_as_compl_or_contr = set()
    already_processed_pair = set()

    compatible_groups = []
    largest_contained_views = set()
    candidate_complementary_contradictory_views = []
    # compl_contra_relation_graph = defaultdict(lambda: defaultdict(tuple))

    hash_dict = {}

    total_identify_c1_time = 0.0
    total_identify_c2_time = 0.0

    start_time = time.time()

    compatible_clusters = defaultdict(set)

    for df, path in dfs:
        df_hash = hash_pandas_object(df, index=False)

        hash_sum = df_hash.sum()

        hash_dict[path] = set(df_hash)

        compatible_clusters[hash_sum].add(path)

    # compatible_count = len(compatible_clusters.keys())

    compatible_views_to_remove = set()
    for k, cluster in compatible_clusters.items():
        compatible_groups.append(cluster.copy())
        # only keep the first compatible view
        cluster.pop()
        compatible_views_to_remove.update(cluster)

    total_identify_c1_time += time.time() - start_time

    start_time = time.time()

    num_comparisons = 0

    for df1, path1 in dfs:

        if path1 in compatible_views_to_remove \
                or path1 in already_classified_as_contained:
            continue

        # compatible_group = [path1]
        # contained_group = [path1]
        largest_contained_view = None

        for df2, path2 in dfs:

            if path2 in compatible_views_to_remove \
                    or path2 in already_classified_as_contained:
                continue

            if (path1, path2) in already_processed_pair:
                continue

            if path1 == path2:
                continue

            hash_set1 = hash_dict[path1]

            hash_set2 = hash_dict[path2]

            # hash_set1 = set(df1_hash)
            # hash_set2 = set(df2_hash)
            num_comparisons += 1
            if hash_set2.issubset(hash_set1):
                # view2 is contained in view1
                if largest_contained_view is None:
                    largest_contained_view = (df1, path1)
                else:
                    if len(df1) > len(largest_contained_view[0]):
                        already_classified_as_contained.add(largest_contained_view[1])
                        largest_contained_view = (df1, path1)
                already_classified_as_contained.add(path2)
            elif hash_set1.issubset(hash_set2):
                # view1 is contained in view2
                if largest_contained_view is None:
                    largest_contained_view = (df2, path2)
                else:
                    if len(df2) > len(largest_contained_view[0]):
                        already_classified_as_contained.add(largest_contained_view[1])
                        largest_contained_view = (df2, path2)
                already_classified_as_contained.add(path1)

            # else:
            #     # if path1 not in already_classified_as_compl_or_contr:
            #     #     candidate_complementary_contradictory_views.append((df1, path1))
            #     #     already_classified_as_compl_or_contr.add(path1)
            #     # if path2 not in already_classified_as_compl_or_contr:
            #     #     candidate_complementary_contradictory_views.append((df2, path2))
            #     #     already_classified_as_compl_or_contr.add(path2)
            #
            #     # Verify that views are potentially complementary
            #     s12 = (hash_set1 - hash_set2)
            #     s1_complement = set()
            #     if len(s12) > 0:
            #         s1_complement.update((s12))
            #     s21 = (hash_set2 - hash_set1)
            #     s2_complement = set()
            #     if len(s21) > 0:
            #         s2_complement.update((s21))
            #
            #     if len(s1_complement) > 0 and len(s2_complement) > 0:  # and, otherwise it's a containment rel
            #
            #         # idx1 = [idx for idx, value in enumerate(hash_set1) if value in s1_complement]
            #         # idx2 = [idx for idx, value in enumerate(hash_set2) if value in s2_complement]
            #
            #         if path1 not in already_classified_as_compl_or_contr:
            #             candidate_complementary_contradictory_views.append((df1, path1))
            #             already_classified_as_compl_or_contr.add(path1)
            #         if path2 not in already_classified_as_compl_or_contr:
            #             candidate_complementary_contradictory_views.append((df2, path2))
            #             already_classified_as_compl_or_contr.add(path2)

            already_processed_pair.add((path1, path2))
            already_processed_pair.add((path2, path1))

        # if len(compatible_group) > 1:
        # compatible_groups.append(compatible_group)
        # compatible_groups.add(path1)
        # compatible_groups.append(compatible_group)

        if largest_contained_view is not None:
            largest_contained_views.add(largest_contained_view[1])

    to_be_removed = compatible_views_to_remove.union(already_classified_as_contained)

    for df, path in dfs:
        if path not in to_be_removed:
            candidate_complementary_contradictory_views.append((df, path))

    total_identify_c2_time += time.time() - start_time

    print(f"total_identify_c1_time: {total_identify_c1_time}")
    print(f"total_identify_c2_time: {total_identify_c2_time}")

    return compatible_groups, compatible_views_to_remove, \
           largest_contained_views, already_classified_as_contained, \
           candidate_complementary_contradictory_views, \
           total_identify_c1_time, total_identify_c2_time, \
           num_comparisons  # ,
    # compl_contra_relation_graph


def identify_compatible_contained_views_new(dfs):
    # already_classified_as_compatible = set()
    already_classified_as_contained = set()
    # already_classified_as_compl_or_contr = set()
    already_processed_pair = set()

    # compatible_groups = []
    largest_contained_views = set()
    candidate_complementary_contradictory_views = []
    # compl_contra_relation_graph = defaultdict(lambda: defaultdict(tuple))

    hash_dict = {}

    # total_identify_c1_time = 0.0
    # total_identify_c2_time = 0.0

    # start_time = time.time()

    compatible_clusters = defaultdict(tuple)

    # for df, path in dfs:
    #
    #     df_hash = hash_pandas_object(df, index=False)
    #
    #     hash_sum = df_hash.sum()
    #
    #     hash_dict[path] = set(df_hash)
    #
    #     compatible_clusters[hash_sum].add(path)
    #
    # # compatible_count = len(compatible_clusters.keys())
    #
    compatible_views_to_remove = set()
    # for k, cluster in compatible_clusters.items():
    #     compatible_groups.append(cluster.copy())
    #     # only keep the first compatible view
    #     cluster.pop()
    #     compatible_views_to_remove.update(cluster)

    # total_identify_c1_time += time.time() - start_time

    # start_time = time.time()

    for df1, path1 in dfs:

        if path1 in already_classified_as_contained:
            continue

        if path1 not in hash_dict.keys():
            df_hash = hash_pandas_object(df1, index=False)
            hash_dict[path1] = set(df_hash)

        hash_set1 = hash_dict[path1]

        hash_sum1 = sum(hash_set1)

        if hash_sum1 in compatible_clusters.keys():
            # print ("compatible set",compatible_views_to_remove)
            (df, path) = compatible_clusters[hash_sum1]
            if path != path1:
                compatible_views_to_remove.add(path1)
                continue
        else:
            compatible_clusters[hash_sum1] = (df1, path1)

        # compatible_group = [path1]
        # contained_group = [path1]
        largest_contained_view = None

        for df2, path2 in dfs:

            if path2 in already_classified_as_contained:
                continue

            if (path1, path2) in already_processed_pair:
                continue

            if path1 == path2:
                continue

            # hash_set1 = hash_dict[path1]

            if path2 not in hash_dict.keys():
                df_hash2 = hash_pandas_object(df2, index=False)
                hash_dict[path2] = set(df_hash2)

            hash_set2 = hash_dict[path2]

            hash_sum2 = sum(hash_set2)

            if hash_sum2 in compatible_clusters.keys():
                (df, path) = compatible_clusters[hash_sum2]
                if path != path2:
                    compatible_views_to_remove.add(path2)
                    continue
            else:
                compatible_clusters[hash_sum2] = (df2, path2)

            # hash_set1 = set(df1_hash)
            # hash_set2 = set(df2_hash)

            if hash_set2.issubset(hash_set1):
                # view2 is contained in view1
                if largest_contained_view is None:
                    largest_contained_view = (df1, path1)
                else:
                    if len(df1) > len(largest_contained_view[0]):
                        already_classified_as_contained.add(largest_contained_view[1])
                        largest_contained_view = (df1, path1)
                already_classified_as_contained.add(path2)
            elif hash_set1.issubset(hash_set2):
                # view1 is contained in view2
                if largest_contained_view is None:
                    largest_contained_view = (df2, path2)
                else:
                    if len(df2) > len(largest_contained_view[0]):
                        already_classified_as_contained.add(largest_contained_view[1])
                        largest_contained_view = (df2, path2)
                already_classified_as_contained.add(path1)

            # else:
            #     # if path1 not in already_classified_as_compl_or_contr:
            #     #     candidate_complementary_contradictory_views.append((df1, path1))
            #     #     already_classified_as_compl_or_contr.add(path1)
            #     # if path2 not in already_classified_as_compl_or_contr:
            #     #     candidate_complementary_contradictory_views.append((df2, path2))
            #     #     already_classified_as_compl_or_contr.add(path2)
            #
            #     # Verify that views are potentially complementary
            #     s12 = (hash_set1 - hash_set2)
            #     s1_complement = set()
            #     if len(s12) > 0:
            #         s1_complement.update((s12))
            #     s21 = (hash_set2 - hash_set1)
            #     s2_complement = set()
            #     if len(s21) > 0:
            #         s2_complement.update((s21))
            #
            #     if len(s1_complement) > 0 and len(s2_complement) > 0:  # and, otherwise it's a containment rel
            #
            #         # idx1 = [idx for idx, value in enumerate(hash_set1) if value in s1_complement]
            #         # idx2 = [idx for idx, value in enumerate(hash_set2) if value in s2_complement]
            #
            #         if path1 not in already_classified_as_compl_or_contr:
            #             candidate_complementary_contradictory_views.append((df1, path1))
            #             already_classified_as_compl_or_contr.add(path1)
            #         if path2 not in already_classified_as_compl_or_contr:
            #             candidate_complementary_contradictory_views.append((df2, path2))
            #             already_classified_as_compl_or_contr.add(path2)

            already_processed_pair.add((path1, path2))
            already_processed_pair.add((path2, path1))

        # if len(compatible_group) > 1:
        # compatible_groups.append(compatible_group)
        # compatible_groups.add(path1)
        # compatible_groups.append(compatible_group)

        if largest_contained_view is not None:
            largest_contained_views.add(largest_contained_view[1])

    compatible_groups = list(compatible_clusters.values())

    to_be_removed = compatible_views_to_remove.union(already_classified_as_contained)

    for df, path in dfs:
        if path not in to_be_removed:
            candidate_complementary_contradictory_views.append((df, path))

    # total_identify_c2_time += time.time() - start_time

    # print(f"total_identify_c1_time: {total_identify_c1_time}")
    # print(f"total_identify_c2_time: {total_identify_c2_time}")

    return compatible_groups, compatible_views_to_remove, \
           largest_contained_views, already_classified_as_contained, \
           candidate_complementary_contradictory_views  # ,
    # compl_contra_relation_graph


def identify_compatible_contained_views(dfs):
    already_classified_as_compatible = set()
    already_classified_as_contained = set()
    already_classified_as_compl_or_contr = set()
    already_processed_pair = set()

    compatible_views = []
    largest_contained_views = set()
    candidate_complementary_contradictory_views = []
    # compl_contra_relation_graph = defaultdict(lambda: defaultdict(tuple))

    hash_dict = {}

    total_hash_time = 0.0
    total_identify_c1_c2_time = 0.0

    # dfs_sorted = sorted(dfs, key=lambda tup: len(tup[1]), reverse=True)

    for df1, path1 in dfs:

        if path1 in already_classified_as_compatible \
                or path1 in already_classified_as_contained:
            continue

        compatible_group = [path1]
        # contained_group = [path1]
        largest_contained_view = None

        for df2, path2 in dfs:

            if path2 in already_classified_as_compatible \
                    or path2 in already_classified_as_contained:
                continue

            if (path1, path2) in already_processed_pair:
                continue

            if path1 == path2:
                continue

            start_time = time.time()

            if path1 in hash_dict:
                # print("in")
                df1_hash = hash_dict[path1]
            else:
                df1_hash = hash_pandas_object(df1, index=False)
                hash_dict[path1] = df1_hash
            hash_sum1 = df1_hash.sum()

            if path2 in hash_dict:
                # print("in")
                df2_hash = hash_dict[path2]
            else:
                df2_hash = hash_pandas_object(df2, index=False)
                hash_dict[path2] = df2_hash
            hash_sum2 = df2_hash.sum()

            total_hash_time += time.time() - start_time

            start_time = time.time()

            if hash_sum1 == hash_sum2:
                # compatible
                compatible_group.append(path2)
                already_classified_as_compatible.add(path1)
                # already_classified_as_compatible.add(path2)
                # to_be_removed.add(path2)
            else:
                hash_set1 = set(df1_hash)
                hash_set2 = set(df2_hash)

                if hash_set2.issubset(hash_set1):
                    # view2 is contained in view1
                    if largest_contained_view is None:
                        largest_contained_view = (df1, path1)
                    else:
                        if len(df1) > len(largest_contained_view[0]):
                            already_classified_as_contained.add(largest_contained_view[1])
                            largest_contained_view = (df1, path1)
                    already_classified_as_contained.add(path2)
                    # to_be_removed.add(path2)
                elif hash_set1.issubset(hash_set2):
                    # view1 is contained in view2
                    if largest_contained_view is None:
                        largest_contained_view = (df2, path2)
                    else:
                        if len(df2) > len(largest_contained_view[0]):
                            already_classified_as_contained.add(largest_contained_view[1])
                            largest_contained_view = (df2, path2)
                    already_classified_as_contained.add(path1)
                    # to_be_removed.add(path1)

                # else:
                #
                #     if path1 not in already_classified_as_compl_or_contr:
                #         candidate_complementary_contradictory_views.append((df1, path1))
                #         already_classified_as_compl_or_contr.add(path1)
                #     if path2 not in already_classified_as_compl_or_contr:
                #         candidate_complementary_contradictory_views.append((df2, path2))
                #         already_classified_as_compl_or_contr.add(path2)

                # # Verify that views are potentially complementary
                # s12 = (hash_set1 - hash_set2)
                # s1_complement = set()
                # if len(s12) > 0:
                #     s1_complement.update((s12))
                # s21 = (hash_set2 - hash_set1)
                # s2_complement = set()
                # if len(s21) > 0:
                #     s2_complement.update((s21))
                #
                # if len(s1_complement) > 0 and len(s2_complement) > 0:  # and, otherwise it's a containment rel
                #
                #     # idx1 = [idx for idx, value in enumerate(hash_set1) if value in s1_complement]
                #     # idx2 = [idx for idx, value in enumerate(hash_set2) if value in s2_complement]
                #
                #     if path1 not in already_classified_as_compl_or_contr:
                #         candidate_complementary_contradictory_views.append((df1, path1))
                #         already_classified_as_compl_or_contr.add(path1)
                #     if path2 not in already_classified_as_compl_or_contr:
                #         candidate_complementary_contradictory_views.append((df2, path2))
                #         already_classified_as_compl_or_contr.add(path2)

                already_processed_pair.add((path1, path2))
                already_processed_pair.add((path2, path1))

            total_identify_c1_c2_time += time.time() - start_time

        # if len(compatible_group) > 1:
        # compatible_views.append(compatible_group)
        # compatible_views.add(path1)
        compatible_views.append(compatible_group)

        if largest_contained_view is not None:
            largest_contained_views.add(largest_contained_view[1])

    to_be_removed = already_classified_as_compatible.union(already_classified_as_contained)

    for df, path in dfs:
        if path not in to_be_removed:
            candidate_complementary_contradictory_views.append((df, path))

    print(f"total_hash_time: {total_hash_time}")
    print(f"total_identify_c1_c2_time: {total_identify_c1_c2_time}")

    return compatible_views, largest_contained_views, already_classified_as_contained, \
           candidate_complementary_contradictory_views  # ,
    # compl_contra_relation_graph


def power_set(iterable, size_range):
    "power_set([1,2,3], (0, 3)) --> () (1,) (2,) (3,) (1,2) (1,3) (2,3) (1,2,3)"
    s = list(iterable)
    return chain.from_iterable(combinations(s, r) for r in range(size_range[0], size_range[1] + 1))


def find_candidate_keys(df, sampling=False, max_num_attr_in_composite_key=2, uniqueness_threshold=0.9):
    candidate_keys = []

    # infer and convert types (originally all columns have 'object' type)
    # print(df.infer_objects().dtypes)
    # df = df.convert_dtypes()
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

    # print(sample.dtypes)
    if max_num_attr_in_composite_key >= len(sample.columns):
        # we don't want the key to be all columns
        max_num_attr_in_composite_key = len(sample.columns) - 1

    possible_keys = power_set(sample.columns.values, (1, max_num_attr_in_composite_key))

    # TODO: pruning
    # Find all candidate keys that have the same/similar maximum strength above some threshold
    max_strength = 0
    epsilon = 1e-8
    for key in map(list, filter(None, possible_keys)):
        # strength of a key = the number of distinct key values in the dataset divided by the number of rows
        num_groups = sample.groupby(key).ngroups
        strength = num_groups / sample_size

        # print(f"candidate key: {key}, uniqueness: {strength}")

        if strength < uniqueness_threshold:
            continue

        if abs(strength - max_strength) < epsilon:
            candidate_keys.append(tuple(key))
        elif strength > max_strength:
            candidate_keys = [tuple(key)]
            max_strength = strength

    # print("candidate keys:", candidate_keys)

    return candidate_keys


def build_inverted_index(dfs, candidate_key_size=2, uniqueness_threshold=0.9):
    candidate_key_to_inverted_index = defaultdict(lambda: defaultdict(list))

    total_find_candidate_keys_time = 0.0
    total_create_inverted_index_time = 0.0

    view_to_candidate_keys_dict = {}

    for df, path in tqdm(dfs):

        start_time = time.time()

        candidate_keys = find_candidate_keys(df, sampling=False,
                                             max_num_attr_in_composite_key=candidate_key_size,
                                             uniqueness_threshold=uniqueness_threshold)
        view_to_candidate_keys_dict[path] = candidate_keys

        total_find_candidate_keys_time += time.time() - start_time

        start_time = time.time()

        # if we didn't find any key (ex. all the columns are float), then we don't classify any
        # contradictory or complementary groups
        if len(candidate_keys) == 0:
            continue

        for candidate_key in candidate_keys:

            key_values = df[list(candidate_key)].values.tolist()
            # print(key_values)

            for i, key_value in enumerate(key_values):
                # print(tuple(key_value))
                candidate_key_to_inverted_index[candidate_key][tuple(key_value)].append((df, path, i))

        total_create_inverted_index_time += time.time() - start_time

    print(f"total_find_candidate_keys_time: {total_find_candidate_keys_time} s")
    print(f"total_create_inverted_index_time: {total_create_inverted_index_time} s")

    return candidate_key_to_inverted_index, view_to_candidate_keys_dict


def identify_complementary_contradictory_views(path_to_df_dict,
                                               candidate_complementary_contradictory_views,
                                               candidate_key_size=2,
                                               find_all_contradictions=True):
    candidate_key_to_inverted_index, view_to_candidate_keys_dict = \
        build_inverted_index(
            candidate_complementary_contradictory_views,
            candidate_key_size)

    start_time = time.time()

    all_pair_result = defaultdict(lambda: defaultdict(set))

    # print(candidate_key_to_inverted_index[tuple("a")][(2,)])

    for candidate_key, inverted_index in tqdm(candidate_key_to_inverted_index.items()):

        # print(candidate_key)

        already_classified_as_contradictory = set()

        for key_value, dfs in tqdm(inverted_index.items()):

            to_be_removed = set()
            already_processed_pairs = set()

            if len(dfs) <= 1:
                # only one view for this key value, no need to compare
                continue

            clusters = defaultdict(set)
            new_contradictions = set()

            for df, path, idx in dfs:
                clusters[path].add(path)

            for df1, path1, idx1 in dfs:

                if path1 in to_be_removed:
                    continue

                for df2, path2, idx2 in dfs:

                    # if candidate_key == tuple("a") and key_value == (2,):
                    # and path1 == "test_dir/view_1.csv" and path2 == "test_dir/view_2.csv":
                    # print("in")

                    if path1 == path2:
                        continue

                    if (path1, path2) in already_processed_pairs:
                        continue

                    # have_contradiction = False
                    # skip = False
                    # if path1 in to_be_removed:
                    #     skip = True
                    #     cluster = clusters[path1]
                    #     for p in cluster:
                    #         if (path2, p) in all_pair_result.keys():
                    #             if candidate_key in all_pair_result[(path2, p)].keys():
                    #                 if len(all_pair_result[(path2, p)][candidate_key]) > 0:
                    #                     have_contradiction = True
                    #                     all_pair_result[(path1, path2)][candidate_key].update(
                    #                         all_pair_result[(path2, p)][candidate_key])
                    #         elif (p, path2) in all_pair_result.keys():
                    #             if candidate_key in all_pair_result[(p, path2)].keys():
                    #                 if len(all_pair_result[(p, path2)][candidate_key]) > 0:
                    #                     have_contradiction = True
                    #                     all_pair_result[(path1, path2)][candidate_key].update(
                    #                         all_pair_result[(p, path2)][candidate_key])
                    #         if have_contradiction:
                    #             # print("in")
                    #             break
                    # if path2 in to_be_removed:
                    #     skip = True
                    #     cluster = clusters[path2]
                    #     for p in cluster:
                    #         if (path1, p) in all_pair_result.keys():
                    #             if candidate_key in all_pair_result[(path1, p)].keys():
                    #                 if len(all_pair_result[(path1, p)][candidate_key]) > 0:
                    #                     have_contradiction = True
                    #                     all_pair_result[(path1, path2)][candidate_key].update(
                    #                         all_pair_result[(path1, p)][candidate_key])
                    #         elif (p, path1) in all_pair_result.keys():
                    #             if candidate_key in all_pair_result[(p, path1)].keys():
                    #                 if len(all_pair_result[(p, path1)][candidate_key]) > 0:
                    #                     have_contradiction = True
                    #                     all_pair_result[(path1, path2)][candidate_key].update(
                    #                         all_pair_result[(p, path1)][candidate_key])
                    #         if have_contradiction:
                    #             # print("in")
                    #             break
                    #
                    # if have_contradiction:
                    #     already_processed_pairs.add((path1, path2))
                    #     already_processed_pairs.add((path2, path1))
                    # if skip:
                    #     continue

                    if not find_all_contradictions:
                        # if path1 in to_be_removed or path2 in to_be_removed:
                        #     continue
                        if (path1, path2) in already_classified_as_contradictory:
                            continue

                    if path2 in to_be_removed:
                        continue

                    # complementary_keys1 = set()
                    # complementary_keys2 = set()
                    # contradictory_keys = set()
                    # all_pair_result[(path1, path2)][candidate_key] = None

                    # print(df1.iloc[idx1].values)
                    # print(df2.iloc[idx2].values)
                    # print(df1.iloc[idx1].values == df2.iloc[idx2].values)

                    if (df1.iloc[idx1].values == df2.iloc[idx2].values).all():
                        # print("in")
                        clusters[path1].add(path2)
                        # clusters[path2].add(path1)
                        if path2 in clusters:
                            del clusters[path2]

                        # if not find_all_contradictions:
                        to_be_removed.add(path1)
                    else:
                        # contradictory_keys.add(key_value)

                        if not find_all_contradictions:
                            already_classified_as_contradictory.add((path1, path2))
                            already_classified_as_contradictory.add((path2, path1))

                        # if (path2, path1) in all_pair_result.keys():
                        #     # print((path1, path2))
                        #     p1 = path1
                        #     path1 = path2
                        #     path2 = p1
                        #     # print((path1, path2))

                        new_contradictions.add((path1, path2))
                        new_contradictions.add((path2, path1))
                        all_pair_result[(path1, path2)][candidate_key].add(key_value)

                    already_processed_pairs.add((path1, path2))
                    already_processed_pairs.add((path2, path1))

            # print(candidate_key, key_value)
            # print(clusters)

            already_added = set()
            for path1 in clusters.keys():
                for path2, cluster in clusters.items():
                    if path1 == path2:
                        continue
                    if (path1, path2) in already_added:
                        continue
                    # if not find_all_contradictions:
                    #     if (path1, path2) in already_classified_as_contradictory:
                    #         continue

                    # if (path2, path1) in all_pair_result.keys() and candidate_key in all_pair_result[(path2, path1)]:
                    #     all_pair_result[(path2, path1)][candidate_key].add(key_value)
                    # else:
                    #     all_pair_result[(path1, path2)][candidate_key].add(key_value)
                    # already_added.add((path1, path2))
                    # already_added.add((path2, path1))
                    # if not find_all_contradictions:
                    #     already_classified_as_contradictory.add((path1, path2))
                    #     already_classified_as_contradictory.add((path2, path1))

                    for p in cluster:
                        if not find_all_contradictions:
                            if (path1, p) in already_classified_as_contradictory:
                                continue

                        # if (p, path1) in all_pair_result.keys():# and candidate_key in all_pair_result[(p, path1)]:
                        #     all_pair_result[(p, path1)][candidate_key].add(key_value)
                        # else:
                        #     all_pair_result[(path1, p)][candidate_key].add(key_value)
                        if (path1, p) not in new_contradictions:
                            all_pair_result[(path1, p)][candidate_key].add(key_value)

                        already_added.add((path1, p))
                        already_added.add((p, path1))

                        if not find_all_contradictions:
                            already_classified_as_contradictory.add((path1, p))
                            already_classified_as_contradictory.add((p, path1))

    print(f"total_find_contradiction_time: {time.time() - start_time} s")

    # print(all_pair_result[("test_dir/view_1.csv", "test_dir/view_2.csv")])
    # print(all_pair_result)

    start_time = time.time()
    # processed_pairs = list(all_pair_result.keys())
    processed_pairs = set()
    for pair in all_pair_result.keys():
        path1, path2 = pair
        processed_pairs.add((path1, path2))
        processed_pairs.add((path2, path1))

    complementary_contradictory_view_paths = [path for df, path in candidate_complementary_contradictory_views]
    complementary_pairs_dup = set(itertools.product(complementary_contradictory_view_paths, repeat=2)) - processed_pairs
    complementary_pairs_dedup = set()
    complementary_pairs = []
    for pair in complementary_pairs_dup:

        path1, path2 = pair
        if path1 == path2:
            continue

        if pair not in complementary_pairs_dedup:

            complementary_pairs_dedup.add((path1, path2))
            complementary_pairs_dedup.add((path2, path1))

            candidate_keys1 = view_to_candidate_keys_dict[path1]
            candidate_keys2 = view_to_candidate_keys_dict[path2]

            if len(candidate_keys1) == 0 and len(candidate_keys2) == 0:
                continue

            candidate_keys = set(candidate_keys1).intersection(set(candidate_keys2))

            for candidate_key in candidate_keys:
                # complementary of if the key values from two views have some overlap
                df1 = path_to_df_dict[path1]
                df2 = path_to_df_dict[path2]
                key_values1 = set(tuple(r) for r in df1[list(candidate_key)].to_numpy())
                key_values2 = set(tuple(r) for r in df2[list(candidate_key)].to_numpy())
                # print("key_values1", key_values1)
                # print("key_values2", key_values2)
                if not key_values1.isdisjoint(key_values2):
                    complementary_pairs.append((path1, path2, candidate_key))
                # else:
                #     print("in")

    contradictory_pairs = []
    for path, v1 in all_pair_result.items():
        path1, path2 = path
        for candidate_key, contradictory_key_values in v1.items():
            if len(contradictory_key_values) == 0:
                complementary_pairs.append((path1, path2, candidate_key))
            else:
                for contradictory_key_value in contradictory_key_values:
                    contradictory_pairs.append((path1, path2, candidate_key, contradictory_key_value))

    print(f"total_find_complementary_time: {time.time() - start_time} s")

    return complementary_pairs, contradictory_pairs, all_pair_result


def identify_complementary_contradictory_views_optimized(path_to_df_dict,
                                                         candidate_complementary_contradictory_views,
                                                         candidate_key_size=2,
                                                         find_all_contradictions=True,
                                                         uniqueness_threshold=0.9):
    candidate_key_to_inverted_index, view_to_candidate_keys_dict = \
        build_inverted_index(
            candidate_complementary_contradictory_views,
            candidate_key_size,
            uniqueness_threshold)

    start_time = time.time()

    all_contradictory_pair_result = defaultdict(lambda: defaultdict(set))

    # print(candidate_key_to_inverted_index[tuple("a")][(2,)])

    for candidate_key, inverted_index in tqdm(candidate_key_to_inverted_index.items()):

        already_classified_as_contradictory = set()

        for key_value, dfs in tqdm(inverted_index.items()):

            if len(dfs) <= 1:
                # only one view for this key value, no need to compare
                continue

            clusters = defaultdict(set)

            for df, path, idx in dfs:
                row = tuple(df.iloc[idx])
                clusters[row].add(path)

            already_added = set()
            for row1, cluster1 in clusters.items():

                for row2, cluster2 in clusters.items():

                    if row1 == row2:
                        continue

                    for path1 in cluster1:
                        for path2 in cluster2:

                            if not find_all_contradictions:
                                if (path1, path2) in already_classified_as_contradictory:
                                    continue
                            if path1 == path2:
                                continue
                            if (path1, path2) in already_added:
                                continue

                            all_contradictory_pair_result[(path1, path2)][candidate_key].add(key_value)

                            already_added.add((path1, path2))
                            already_added.add((path2, path1))

                            if not find_all_contradictions:
                                already_classified_as_contradictory.add((path1, path2))
                                already_classified_as_contradictory.add((path2, path1))

    print(f"total_find_contradiction_time: {time.time() - start_time} s")

    # print(all_contradictory_pair_result[("test_dir/view_1.csv", "test_dir/view_2.csv")])
    # print(all_contradictory_pair_result)

    start_time = time.time()
    # processed_pairs = list(all_contradictory_pair_result.keys())
    processed_pairs = set()
    for pair in all_contradictory_pair_result.keys():
        path1, path2 = pair
        processed_pairs.add((path1, path2))
        processed_pairs.add((path2, path1))

    complementary_contradictory_view_paths = [path for df, path in candidate_complementary_contradictory_views]
    complementary_pairs_dup = set(itertools.product(complementary_contradictory_view_paths, repeat=2)) - processed_pairs
    complementary_pairs_dedup = set()
    complementary_pairs = []
    key_values_dict = defaultdict(lambda: defaultdict(set))

    for pair in tqdm(complementary_pairs_dup):

        path1, path2 = pair
        if path1 == path2:
            continue

        if pair not in complementary_pairs_dedup:

            complementary_pairs_dedup.add((path1, path2))
            complementary_pairs_dedup.add((path2, path1))

            candidate_keys1 = view_to_candidate_keys_dict[path1]
            candidate_keys2 = view_to_candidate_keys_dict[path2]

            if len(candidate_keys1) == 0 and len(candidate_keys2) == 0:
                continue

            candidate_keys = set(candidate_keys1).intersection(set(candidate_keys2))

            for candidate_key in candidate_keys:
                # complementary only if the key values from two views have some overlap
                df1 = path_to_df_dict[path1]
                df2 = path_to_df_dict[path2]

                if path1 in key_values_dict.keys() and candidate_key in key_values_dict[path1].keys():
                    key_values1 = key_values_dict[path1][candidate_key]
                else:
                    key_values1 = set(tuple(r) for r in df1[list(candidate_key)].to_numpy())
                    key_values_dict[path1][candidate_key] = key_values1

                if path2 in key_values_dict.keys() and candidate_key in key_values_dict[path2].keys():
                    key_values2 = key_values_dict[path2][candidate_key]
                else:
                    key_values2 = set(tuple(r) for r in df2[list(candidate_key)].to_numpy())
                    key_values_dict[path2][candidate_key] = key_values2

                # print("key_values1", key_values1)
                # print("key_values2", key_values2)
                if not key_values1.isdisjoint(key_values2):
                    complementary_pairs.append((path1, path2, candidate_key))
                # else:
                #     print("in")

    contradictory_pairs = []
    for path, v1 in all_contradictory_pair_result.items():
        path1, path2 = path
        for candidate_key, contradictory_key_values in v1.items():
            if len(contradictory_key_values) == 0:
                complementary_pairs.append((path1, path2, candidate_key))
            else:
                for contradictory_key_value in contradictory_key_values:
                    contradictory_pairs.append((path1, path2, candidate_key, contradictory_key_value))

    print(f"total_find_complementary_time: {time.time() - start_time} s")

    return complementary_pairs, contradictory_pairs, all_contradictory_pair_result


def main(input_path, view_paths=None, candidate_key_size=2, find_all_contradictions=True, uniqueness_threshold=0.9):
    start_time = time.time()
    dfs, path_to_df_dict, total_num_rows = get_dataframes(input_path, view_paths)
    get_df_time = time.time() - start_time
    print(f"get_dataframes time: {get_df_time} s")
    print("Found " + str(len(dfs)) + " valid views")

    start_time = time.time()
    dfs_per_schema, schema_id_info = classify_per_table_schema(dfs)
    find_compatible_contained_time = time.time() - start_time
    print(f"classify_per_table_schema time: {find_compatible_contained_time} s")
    print("View candidates classify into " + str(len(dfs_per_schema)) + " groups based on schema")

    compatible_groups = []
    removed_compatible_views = []
    contained_groups = []
    removed_contained_views = []
    complementary_groups = []
    contradictory_groups = []
    all_contradictory_pair_results = {}
    find_compatible_contained_time_total = 0.0
    find_complementary_contradictory_time_total = 0.0
    schema_group = []
    total_identify_c1_time = 0.0
    total_identify_c2_time = 0.0
    total_num_comparisons_c2 = 0

    for key, group_dfs in dfs_per_schema.items():
        print("Num elements with schema " + str(key) + " is: " + str(len(group_dfs)))
        schema_group.append(len(group_dfs))

        start_time = time.time()
        compatible_views, compatible_views_to_remove, \
        largest_contained_views, contained_views_to_remove, \
        candidate_complementary_contradictory_views, \
        identify_c1_time, identify_c2_time, num_comparisons = \
            identify_compatible_contained_views_optimized(group_dfs)
        find_compatible_contained_time = time.time() - start_time
        print(f"identify_compatible_contained_views time: {find_compatible_contained_time} s")
        find_compatible_contained_time_total += find_compatible_contained_time
        total_identify_c1_time += identify_c1_time
        total_identify_c2_time += identify_c2_time
        total_num_comparisons_c2 += num_comparisons

        # print(f"number of compatible groups: {len(compatible_views)}")
        # print(compatible_views)
        # print(f"number of contained groups: {len(largest_contained_views)}")
        # print(contained_views)

        start_time = time.time()
        complementary_pairs, contradictory_pairs, all_contradictory_pair_result = \
            identify_complementary_contradictory_views_optimized(path_to_df_dict,
                                                                 candidate_complementary_contradictory_views,
                                                                 candidate_key_size=candidate_key_size,
                                                                 find_all_contradictions=find_all_contradictions,
                                                                 uniqueness_threshold=uniqueness_threshold)
        find_complementary_contradictory_time = time.time() - start_time
        print(f"identify_complementary_contradictory_views time: {find_complementary_contradictory_time} s")
        find_complementary_contradictory_time_total += find_complementary_contradictory_time
        # print(f"number of contradictory pairs: {len(contradictory_pairs)}")
        # print(f"number of complementary pairs: {len(complementary_pairs)}")
        # pprint.pprint(complementary_pairs)
        # pprint.pprint(result)

        compatible_groups += compatible_views
        removed_compatible_views += list(compatible_views_to_remove)
        contained_groups += list(largest_contained_views)
        removed_contained_views += list(contained_views_to_remove)
        complementary_groups.append(complementary_pairs)
        contradictory_groups.append(contradictory_pairs)
        all_contradictory_pair_results.update(all_contradictory_pair_result)

    return path_to_df_dict, \
           compatible_groups, removed_compatible_views, \
           contained_groups, removed_contained_views, \
           complementary_groups, \
           contradictory_groups, \
           all_contradictory_pair_results, \
           find_compatible_contained_time_total, \
           total_identify_c1_time, total_identify_c2_time, total_num_comparisons_c2, \
           find_complementary_contradictory_time_total, \
           schema_group, total_num_rows


if __name__ == "__main__":

    groups = ["compatible", "contained", "contradictory", "complementary"]
    times = []

    for group in groups:
        input_path = f"/Users/zhiruzhu/Desktop/Niffler/aurum-dod-staging/DoD/new_ver_4c/synthetic_views/{group}/"

        start_time = time.time()
        main(input_path)
        elapsed = time.time() - start_time
        print(f"total 4c time for {group}: {elapsed} s")
        times.append(elapsed)

    print(times)
