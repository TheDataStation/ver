import pprint
import time
from os import listdir
from os.path import isfile, join
from collections import defaultdict
import pandas as pd
from pandas.util import hash_pandas_object
import DoD.material_view_analysis as mva

from tqdm import tqdm
from itertools import chain, combinations


def normalize(df):
    # df = df.astype(str).apply(lambda x: x.str.strip().str.lower())
    # infer and convert types (originally all columns have 'object' type)
    # print(df.infer_objects().dtypes)
    # df = df.convert_dtypes(convert_string=False, convert_integer=False)
    df = df.convert_dtypes()
    # print(df.dtypes)
    df = df.apply(lambda x: x.astype(str).str.strip().str.lower() if (x.dtype == 'object') else x)
    # df = df.convert_dtypes()
    # print(df.columns.dtype)
    # print(df)
    return df


def get_dataframes(path):
    files = [path + f for f in listdir(path) if isfile(join(path, f)) and f != '.DS_Store' and f != "log.txt"]
    dfs = []
    for f in files:
        df = pd.read_csv(f, encoding='latin1', thousands=',')  # .replace('"','', regex=True)
        df = mva.curate_view(df)
        df = normalize(df)
        if len(df) > 0:  # only append valid df
            dfs.append((df, f))
    return dfs


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


def identify_compatible_contained_views(dfs):
    already_classified_as_compatible = set()
    already_classified_as_contained = set()
    already_classified_as_compl_or_contr = set()
    already_processed_pair = set()

    compatible_views = []
    contained_views = set()
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
                    or path2 in already_classified_as_contained\
                    or (path1, path2) in already_processed_pair:
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
                already_classified_as_compatible.add(path2)
            else:
                hash_set1 = set(df1_hash)
                hash_set2 = set(df2_hash)

                if hash_set2.issubset(hash_set1):
                    # view2 is contained in view1
                    if largest_contained_view is None:
                        largest_contained_view = (df1, path1)
                    else:
                        if len(df1) > len(largest_contained_view[0]):
                            largest_contained_view = (df1, path1)
                    already_classified_as_contained.add(path2)
                elif hash_set1.issubset(hash_set2):
                    # view1 is contained in view2
                    if largest_contained_view is None:
                        largest_contained_view = (df2, path2)
                    else:
                        if len(df2) > len(largest_contained_view[0]):
                            largest_contained_view = (df2, path2)
                    already_classified_as_contained.add(path1)
                else:

                    # if (path1 + "%%%" + path2) in already_processed_compl_contr_pairs \
                    #         or (path2 + "%%%" + path1) in already_processed_compl_contr_pairs:
                    #     continue  # already processed, skip computation

                    if path1 not in already_classified_as_compl_or_contr:
                        candidate_complementary_contradictory_views.append((df1, path1))
                        already_classified_as_compl_or_contr.add(path1)
                    if path2 not in already_classified_as_compl_or_contr:
                        candidate_complementary_contradictory_views.append((df2, path2))
                        already_classified_as_compl_or_contr.add(path2)

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
                    #     # candidate_complementary_contradictory_views.append((df1, path1, idx1, df2, path2, idx2))
                    #     candidate_complementary_contradictory_views.append((df1, path1))
                    #     candidate_complementary_contradictory_views.append((df2, path2))
                    #
                    #     # compl_contra_relation_graph[path1][path2] = (df1, idx1, df2, idx2)
                    #     # compl_contra_relation_graph[path2][path1] = (df2, idx2, df1, idx1)
                    #
                    #     already_processed_compl_contr_pairs.add((path1 + "%%%" + path2))
                    #     already_processed_compl_contr_pairs.add((path2 + "%%%" + path1))

            already_processed_pair.add((path1, path2))
            already_processed_pair.add((path2, path1))


            total_identify_c1_c2_time += time.time() - start_time

        if len(compatible_group) > 1:
            compatible_views.append(compatible_group)

        if largest_contained_view is not None:
            contained_views.add(largest_contained_view[1])

    print(f"total_hash_time: {total_hash_time}")
    print(f"total_identify_c1_c2_time: {total_identify_c1_c2_time}")

    return compatible_views, contained_views, candidate_complementary_contradictory_views  # ,
    # compl_contra_relation_graph


def power_set(iterable, size_range):
    "power_set([1,2,3], (0, 3)) --> () (1,) (2,) (3,) (1,2) (1,3) (2,3) (1,2,3)"
    s = list(iterable)
    return chain.from_iterable(combinations(s, r) for r in range(size_range[0], size_range[1] + 1))


def find_candidate_keys(df, sampling=False, max_num_attr_in_composite_key=2):
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
    # Find all candidate keys that have the same/similar maximum strength
    max_strength = 0
    epsilon = 1e-8
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


def identify_complementary_contradictory_views(candidate_complementary_contradictory_views,
                                               candidate_key_size=2):
    candidate_key_to_inverted_index = defaultdict(lambda: defaultdict(list))
    # already_added = set()
    # all_candidate_views = []

    # for df1, path1, idx1, df2, path2, idx2 in candidate_complementary_contradictory_views:
    #     if path1 not in already_added:
    #         all_candidate_views.append((df1, path1))
    #     if path2 not in already_added:
    #         all_candidate_views.append((df2, path2))

    total_find_candidate_keys_time = 0.0
    total_create_inverted_index_time = 0.0

    # print(len(candidate_complementary_contradictory_views))
    # for df, path in tqdm(candidate_complementary_contradictory_views):
    #     print(path)

    for df, path in tqdm(candidate_complementary_contradictory_views):

        start_time = time.time()

        candidate_keys = find_candidate_keys(df, sampling=False, max_num_attr_in_composite_key=candidate_key_size)

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

    start_time = time.time()

    all_pair_result = defaultdict(lambda: defaultdict(set))

    # print(candidate_key_to_inverted_index.keys())

    for candidate_key, inverted_index in tqdm(candidate_key_to_inverted_index.items()):

        # print(candidate_key)

        already_classified_as_contradictory = set()

        for key_value, dfs in tqdm(inverted_index.items()):

            to_be_removed = set()

            for df1, path1, idx1 in dfs:
                for df2, path2, idx2 in dfs:

                    if path1 == path2 or (path1, path2) in already_classified_as_contradictory \
                            or path1 in to_be_removed or path2 in to_be_removed:
                        continue

                    # complementary_keys1 = set()
                    # complementary_keys2 = set()
                    contradictory_keys = set()

                    if (df1.iloc[idx1].values == df2.iloc[idx2].values).all(axis=1):
                        # print("in")
                        to_be_removed.add(path1)
                    else:
                        contradictory_keys.add(key_value)
                        all_pair_result[(path1, path2)][candidate_key] = contradictory_keys
                        already_classified_as_contradictory.add((path1, path2))
                        already_classified_as_contradictory.add((path2, path1))

    print(f"total_find_contradiction_time: {time.time() - start_time} s")

    return all_pair_result



def main(input_path, candidate_key_size=2):
    start_time = time.time()
    dfs = get_dataframes(input_path)
    elapsed = time.time() - start_time
    print(f"get_dataframes time: {elapsed} s")
    print("Found " + str(len(dfs)) + " valid tables")

    start_time = time.time()
    dfs_per_schema, schema_id_info = classify_per_table_schema(dfs)
    elapsed = time.time() - start_time
    print(f"classify_per_table_schema time: {elapsed} s")
    print("View candidates classify into " + str(len(dfs_per_schema)) + " groups based on schema")

    for key, group_dfs in dfs_per_schema.items():
        print("Num elements with schema " + str(key) + " is: " + str(len(group_dfs)))

        start_time = time.time()
        compatible_views, contained_views, candidate_complementary_contradictory_views = \
            identify_compatible_contained_views(group_dfs)
        elapsed = time.time() - start_time
        print(f"identify_compatible_contained_views time: {elapsed} s")

        print(f"number of compatible groups: {len(compatible_views)}")
        # print(compatible_views)
        print(f"number of contained groups: {len(contained_views)}")
        # print(contained_views)

        start_time = time.time()
        result = identify_complementary_contradictory_views(candidate_complementary_contradictory_views)
        elapsed = time.time() - start_time
        print(f"identify_complementary_contradictory_views time: {elapsed} s")
        print(f"number of contradictory groups: {len(result)}")
        # pprint.pprint(result)


if __name__ == "__main__":
    input_path = "/Users/zhiruzhu/Desktop/Niffler/aurum-dod-staging/DoD/new_ver_4c/synthetic_views/contained/"

    start_time = time.time()
    main(input_path)
    elapsed = time.time() - start_time
    print(f"total 4c time: {elapsed} s")

