import itertools
import time
from os import listdir
from os.path import isfile, join
from pandas.util import hash_pandas_object

from itertools import chain, combinations
from utils import *


def get_dataframes(path):
    files = [path + f for f in listdir(path) if isfile(join(path, f)) and f != '.DS_Store' and f != "log.txt"]
    dfs = []
    path_to_df_dict = {}
    for f in files:
        df = pd.read_csv(f, encoding='latin1', thousands=',')  # .replace('"','', regex=True)
        df = curate_view(df)
        df = normalize(df)
        if len(df) > 0:  # only append valid df
            dfs.append((df, f))
        path_to_df_dict[f] = df
    return dfs, path_to_df_dict


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

    compatible_views = set()
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
                    or path2 in already_classified_as_contained \
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
                    #     if path1 not in already_classified_as_compl_or_contr:
                    #         candidate_complementary_contradictory_views.append((df1, path1))
                    #         already_classified_as_compl_or_contr.add(path1)
                    #     if path2 not in already_classified_as_compl_or_contr:
                    #         candidate_complementary_contradictory_views.append((df2, path2))
                    #         already_classified_as_compl_or_contr.add(path2)


            already_processed_pair.add((path1, path2))
            already_processed_pair.add((path2, path1))

            total_identify_c1_c2_time += time.time() - start_time

        if len(compatible_group) > 1:
            # compatible_views.append(compatible_group)
            compatible_views.add(path1)

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


def build_inverted_index(dfs, candidate_key_size=2):
    candidate_key_to_inverted_index = defaultdict(lambda: defaultdict(list))

    total_find_candidate_keys_time = 0.0
    total_create_inverted_index_time = 0.0

    view_to_candidate_keys_dict = {}

    for df, path in tqdm(dfs):

        start_time = time.time()

        candidate_keys = find_candidate_keys(df, sampling=False, max_num_attr_in_composite_key=candidate_key_size)
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


def identify_complementary_contradictory_views(candidate_complementary_contradictory_views,
                                               candidate_key_size=2,
                                               find_all_contradictions=False):
    candidate_key_to_inverted_index, view_to_candidate_keys_dict = \
        build_inverted_index(
            candidate_complementary_contradictory_views,
            candidate_key_size)

    start_time = time.time()

    all_pair_result = defaultdict(lambda: defaultdict(set))

    # print(candidate_key_to_inverted_index.keys())

    for candidate_key, inverted_index in tqdm(candidate_key_to_inverted_index.items()):

        # print(candidate_key)

        already_classified_as_contradictory = set()

        for key_value, dfs in tqdm(inverted_index.items()):

            to_be_removed = set()

            if len(dfs) <= 1:
                # only one view for this key value, no need to compare
                continue

            for df1, path1, idx1 in dfs:
                for df2, path2, idx2 in dfs:

                    if path1 == path2 or path1 in to_be_removed or path2 in to_be_removed:
                        continue

                    if not find_all_contradictions:
                        if (path1, path2) in already_classified_as_contradictory:
                            continue
                    # complementary_keys1 = set()
                    # complementary_keys2 = set()
                    # contradictory_keys = set()
                    # all_pair_result[(path1, path2)][candidate_key] = None

                    if (df1.iloc[idx1].values == df2.iloc[idx2].values).all(axis=1):
                        # print("in")
                        to_be_removed.add(path1)
                    else:
                        # contradictory_keys.add(key_value)
                        all_pair_result[(path1, path2)][candidate_key].add(key_value)
                        if not find_all_contradictions:
                            already_classified_as_contradictory.add((path1, path2))
                            already_classified_as_contradictory.add((path2, path1))

    print(f"total_find_contradiction_time: {time.time() - start_time} s")

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
                complementary_pairs.append((path1, path2, candidate_key))

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


def main(input_path, candidate_key_size=2, find_all_contradictions=False):
    start_time = time.time()
    dfs, path_to_df_dict = get_dataframes(input_path)
    elapsed = time.time() - start_time
    print(f"get_dataframes time: {elapsed} s")
    print("Found " + str(len(dfs)) + " valid tables")

    start_time = time.time()
    dfs_per_schema, schema_id_info = classify_per_table_schema(dfs)
    elapsed = time.time() - start_time
    print(f"classify_per_table_schema time: {elapsed} s")
    print("View candidates classify into " + str(len(dfs_per_schema)) + " groups based on schema")

    compatible_groups = set()
    contained_groups = set()
    complementary_groups = []
    contradictory_groups = []
    all_pair_results = {}

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
        complementary_pairs, contradictory_pairs, all_pair_result = \
            identify_complementary_contradictory_views(candidate_complementary_contradictory_views,
                                                       candidate_key_size=candidate_key_size,
                                                       find_all_contradictions=find_all_contradictions)
        elapsed = time.time() - start_time
        print(f"identify_complementary_contradictory_views time: {elapsed} s")
        print(f"number of contradictory pairs: {len(contradictory_pairs)}")
        print(f"number of complementary pairs: {len(complementary_pairs)}")
        # pprint.pprint(complementary_pairs)
        # pprint.pprint(result)

        compatible_groups.update(compatible_views)
        contained_groups.update(contained_views)
        complementary_groups.append(complementary_pairs)
        contradictory_groups.append(contradictory_pairs)
        all_pair_results.update(all_pair_result)

    return path_to_df_dict, compatible_groups, contained_groups, complementary_groups, contradictory_groups, all_pair_results


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
