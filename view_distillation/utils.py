import random
from collections import namedtuple, defaultdict

import numpy as np
import pandas as pd
from tqdm import tqdm


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


def curate_view(df, drop_duplicates=True, dropna=True):
    # df.columns = df.iloc[0]
    # df = df.drop(index=0).reset_index(drop=True)

    if dropna:
        df = df.dropna()  # drop nan
    else:
        df = df.fillna("nan")

    if drop_duplicates:
        df = df.drop_duplicates()
    # this may tweak indexes, so need to reset that
    df = df.reset_index(drop=True)
    # make sure it's sorted according to some order
    df.sort_index(inplace=True, axis=1)
    df.sort_index(inplace=True, axis=0)
    return df


def get_row_from_key(df, key_name, key_value):
    # select row based on multiple composite keys
    assert len(key_value) == len(key_name)

    condition = (df[key_name[0]] == key_value[0])
    for i in range(1, len(key_name)):
        condition = (condition & (df[key_name[i]] == key_value[i]))

    # there may be multiple rows satisfying the same condition
    row = df.loc[condition]
    return row


def row_df_to_string(row_df):
    # there may be multiple rows satisfying the same condition
    df_str = row_df.to_string(header=False, index=False, index_names=False).split('\n')
    row_strs = [','.join(row.split()) for row in df_str]

    # row_strs = []
    # for i in range(len(row_df)):
    #     row = row_df.iloc[[i]]
    #     row_str = row.to_string(header=False, index=False, index_names=False)
    #     row_strs.append(row_str)
    # print(row_strs)
    return row_strs


def add_to_row_to_path_dict(row_to_path_dict, row_strs, path):
    for row in row_strs:
        if row not in row_to_path_dict.keys():
            row_to_path_dict[row] = {path}
        else:
            row_to_path_dict[row].add(path)


def create_contradictory_signals(path_to_df_dict,
                                 view_files,
                                 contradictory_groups,
                                 sample_size=5):
    # contradictions[key][(row1, row2)] = (row1_df, row2_df, views1, views2)
    Contradiction = namedtuple("Contradiction", ["row1_df", "row2_df", "views1", "views2"])
    contradictions = defaultdict(lambda: defaultdict(Contradiction))

    # key: distinct row in all views, value: set of views containing the row
    row_to_path_dict = {}

    # views that are contradictory
    contr_views = set()

    candidate_keys = set()

    for path1, path2, candidate_key_tuple, contradictory_key_value in tqdm(contradictory_groups):

        contr_views.add(path1)
        contr_views.add(path2)

        if not (path1 in view_files and path2 in view_files):
            continue

        df1 = path_to_df_dict[path1]
        df2 = path_to_df_dict[path2]

        # a list containing candidate key names
        candidate_key = list(candidate_key_tuple)

        # select row based on multiple composite keys
        row1_df = get_row_from_key(df1, candidate_key, contradictory_key_value)
        row2_df = get_row_from_key(df2, candidate_key, contradictory_key_value)

        # In some case one key can correspond to more than 1 row, we only take the first row as one row
        # will be sufficient
        row1_df = row1_df.head(1)
        row2_df = row2_df.head(1)
        row1_str = row_df_to_string(row1_df)[0]
        row2_str = row_df_to_string(row2_df)[0]

        p1 = path1
        p2 = path2
        contradiction_already_exist = False
        if candidate_key_tuple in contradictions.keys():
            if (row1_str, row2_str) in contradictions[candidate_key_tuple]:
                contradiction_already_exist = True
            elif (row2_str, row1_str) in contradictions[candidate_key_tuple]:
                row1_str, row2_str = row2_str, row1_str
                row1_df, row2_df = row2_df, row1_df
                p1, p2 = path2, path1
                contradiction_already_exist = True

        if contradiction_already_exist:
            contradiction = contradictions[candidate_key_tuple][(row1_str, row2_str)]
            contradiction.views1.add(p1)
            contradiction.views2.add(p2)
        else:
            contradiction = Contradiction(row1_df=row1_df, row2_df=row2_df, views1={p1}, views2={p2})

        contradictions[candidate_key_tuple][(row1_str, row2_str)] = contradiction

    non_contr_or_compl_view_files = view_files - contr_views
    singletons = []
    for path in non_contr_or_compl_view_files:

        df = path_to_df_dict[path]

        row_strs = row_df_to_string(df)
        add_to_row_to_path_dict(row_to_path_dict, row_strs, path)

        sample_df = df
        sample_size = 5
        if len(df) > sample_size:
            sample_df = df.sample(n=sample_size)

        singletons.append((path, sample_df))

    # pprint.pprint(contradictions)
    # pprint.pprint(complements)
    signals = {}
    signals["contradictions"] = contradictions
    signals["singletons"] = singletons

    return signals, list(candidate_keys)


def pick_best_signal_eval(signals):
    def compute_informativeness(split):
        expected_value = 0.0
        split_size = sum(split)
        if split_size == 0:
            return 0
        for num in split:
            expected_value += num / split_size * num
        return expected_value

    max_informativeness = -1
    epsilon = 1e-8
    candidate_best_signal = []
    candidate_signal_to_delete = []
    for signal_type, s in signals.items():

        if signal_type == "contradictions" or signal_type == "complements":

            for key in s.keys():
                for row_tuple, vtuple in s[key].items():

                    split = [len(vtuple.views1), len(vtuple.views2)]

                    informativeness = compute_informativeness(split)
                    # print(split, informativeness)

                    # if informativeness > max_informativeness:
                    #     max_informativeness = informativeness

                    if abs(informativeness - max_informativeness) < epsilon:
                        best_signal = (signal_type, s[key][row_tuple], key)
                        signal_to_delete = (key, row_tuple)
                        candidate_best_signal.append(best_signal)
                        candidate_signal_to_delete.append(signal_to_delete)
                    elif informativeness > max_informativeness:
                        best_signal = (signal_type, s[key][row_tuple], key)
                        signal_to_delete = (key, row_tuple)
                        candidate_best_signal = [best_signal]
                        candidate_signal_to_delete = [signal_to_delete]
                        max_informativeness = informativeness

        # TODO: force the singleton signal to always be the first to present
        elif signal_type == "singletons":
            views = [view for view, _ in s]

            if len(views) == 0:
                continue

            # one view per branch
            split = [1] * len(views)

            informativeness = compute_informativeness(split)

            if abs(informativeness - max_informativeness) < epsilon:
                best_signal = (signal_type, s, None)
                candidate_best_signal.append(best_signal)
                candidate_signal_to_delete.append(None)
            elif informativeness > max_informativeness:
                best_signal = (signal_type, s, None)
                candidate_best_signal = [best_signal]
                candidate_signal_to_delete = [None]
                max_informativeness = informativeness

    # print(max_informativeness)
    # print(candidate_best_signal)

    best_signal = None
    # print(len(candidate_best_signal))
    if len(candidate_best_signal) > 0:
        random_idx = random.randint(0, len(candidate_best_signal) - 1)
        best_signal = candidate_best_signal[random_idx]
        if best_signal[0] == "singletons":
            del signals["singletons"]
        else:
            signal_to_delete = candidate_signal_to_delete[random_idx]
            del signals[best_signal[0]][signal_to_delete[0]][signal_to_delete[1]]

    # print(best_signal)

    return best_signal


def create_contradictory_signals_multi_row(path_to_df_dict,
                                           view_files,
                                           all_pair_results,
                                           sample_size=5):
    # contradictions[key][(row1, row2)] = (row1_df, row2_df, views1, views2)
    Contradiction = namedtuple("Contradiction", ["row1_df", "row2_df", "views1", "views2"])
    contradictions = defaultdict(lambda: defaultdict(Contradiction))

    # key: distinct row in all views, value: set of views containing the row
    row_to_path_dict = {}

    # views that are contradictory
    contr_views = set()

    candidate_keys = set()

    contradictions_view_pair_dict = defaultdict(lambda: defaultdict(set))

    for path, result in tqdm(all_pair_results.items()):

        path1 = path[0]
        path2 = path[1]

        contr_views.add(path1)
        contr_views.add(path2)

        if not (path1 in view_files and path2 in view_files):
            continue

        df1 = path_to_df_dict[path1]
        df2 = path_to_df_dict[path2]

        for candidate_key_tuple, contradictory_keys in result.items():

            candidate_keys.add(candidate_key_tuple)

            if len(contradictory_keys) > 0:

                # a list containing candidate key names
                candidate_key = list(candidate_key_tuple)
                # a list of tuples, each tuple corresponds to the contradictory key values
                key_values = list(contradictory_keys)

                if len(key_values) > sample_size:
                    key_values = random.sample(key_values, k=sample_size)

                # print(len(key_values))
                sampled_rows = set()

                # there could be multiple contradictions existing in a pair of views
                for key_value in key_values:
                    # select row based on multiple composite keys
                    row1_df = get_row_from_key(df1, candidate_key, key_value)
                    row2_df = get_row_from_key(df2, candidate_key, key_value)

                    # In some case one key can correspond to more than 1 row, we only take the first row as one row
                    # will be sufficient
                    row1_df = row1_df.head(1)
                    row2_df = row2_df.head(1)
                    row1_str = row_df_to_string(row1_df)[0]
                    row2_str = row_df_to_string(row2_df)[0]

                    p1 = path1
                    p2 = path2
                    contradiction_already_exist = False
                    if candidate_key_tuple in contradictions.keys():
                        if (row1_str, row2_str) in contradictions[candidate_key_tuple]:
                            contradiction_already_exist = True
                        elif (row2_str, row1_str) in contradictions[candidate_key_tuple]:
                            row1_str, row2_str = row2_str, row1_str
                            row1_df, row2_df = row2_df, row1_df
                            p1, p2 = path2, path1
                            contradiction_already_exist = True

                    if contradiction_already_exist:
                        contradiction = contradictions[candidate_key_tuple][(row1_str, row2_str)]
                        contradiction.views1.add(p1)
                        contradiction.views2.add(p2)
                    else:
                        contradiction = Contradiction(row1_df=row1_df, row2_df=row2_df, views1={p1}, views2={p2})

                    contradictions[candidate_key_tuple][(row1_str, row2_str)] = contradiction
                    sampled_rows.add((row1_str, row2_str))

                # only skip this signal if the entire rows are the subset of rows in other signal
                # TODO: we could keep the rows that didn't appear in other signals but then we have to resampling,
                #  which has other implications: if we can't resample enough, we are decreasing this signal's
                #  information gain
                skip_this_pair = False
                for view_pair, row_tuples in contradictions_view_pair_dict[candidate_key_tuple].items():
                    if sampled_rows.issubset(row_tuples):
                        skip_this_pair = True
                        break
                if not skip_this_pair:
                    contradictions_view_pair_dict[candidate_key_tuple][(path1, path2)].update(sampled_rows)

    contradictions_new = defaultdict(lambda: defaultdict(Contradiction))
    for candidate_key_tuple in tqdm(contradictions.keys()):
        for view_pair, row_tuples in contradictions_view_pair_dict[candidate_key_tuple].items():
            dfs1 = []
            dfs2 = []
            combined_views1 = set()
            combined_views2 = set()

            for row_tuple in row_tuples:
                contradiction = contradictions[candidate_key_tuple][row_tuple]
                row1_df, row2_df, views1, views2 = contradiction
                dfs1.append(row1_df)
                dfs2.append(row2_df)
                combined_views1.update(views1)
                combined_views2.update(views2)

            df1 = pd.concat(dfs1, ignore_index=True)
            df2 = pd.concat(dfs2, ignore_index=True)

            df1 = curate_view(df1, drop_duplicates=False)
            df1 = normalize(df1)
            df2 = curate_view(df2, drop_duplicates=False)
            df2 = normalize(df2)

            contradiction_multi_row = Contradiction(row1_df=df1, row2_df=df2, views1=combined_views1,
                                                    views2=combined_views2)
            contradictions_new[candidate_key_tuple][view_pair] = contradiction_multi_row
            # print(contradiction_multi_row)
            # print(len(df1))
            # print(len(df2))
            # print()
    # pprint.pprint(contradictions)
    contradictions = contradictions_new
    # pprint.pprint(contradictions)

    non_contr_or_compl_view_files = view_files - contr_views
    singletons = []
    for path in non_contr_or_compl_view_files:

        df = path_to_df_dict[path]

        row_strs = row_df_to_string(df)
        add_to_row_to_path_dict(row_to_path_dict, row_strs, path)

        sample_df = df
        if len(df) > sample_size:
            sample_df = df.sample(n=sample_size)
        singletons.append((path, sample_df))

    # pprint.pprint(contradictions)
    # pprint.pprint(complements)
    signals = {}
    signals["contradictions"] = contradictions
    signals["singletons"] = singletons

    return signals, list(candidate_keys)
