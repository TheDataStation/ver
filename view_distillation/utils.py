import os.path
import random
import time
from collections import namedtuple, defaultdict
from itertools import chain, combinations

import numpy as np
import pandas as pd
from tqdm import tqdm
from os import listdir
from os.path import isfile, join


def normalize(df):
    df = df.convert_dtypes()
    df = df.apply(lambda x: x.astype(str).str.strip().str.lower() if (x.dtype == 'object') else x)
    return df


def curate_view(df, drop_duplicates=True, dropna=True):
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


def get_dataframes(path, dfs=None, dropna=True):
    if dfs is not None:
        files = [f"view{i}" for i in range(len(dfs))]
    else:
        files = [f for f in listdir(path) if
                 isfile(os.path.join(path, f)) and f != '.DS_Store' and ".csv" in f]  # and f != "log.txt"]

    new_dfs = []
    path_to_df_dict = {}

    for i in range(len(files)):
        f = files[i]
        if dfs is not None:
            df = dfs[i]
        else:
            df = pd.read_csv(os.path.join(path, f), encoding='latin1', thousands=',')  # .replace('"','', regex=True)
        df = curate_view(df, dropna=dropna)
        df = normalize(df)
        if len(df) > 0:  # only append valid df
            new_dfs.append((df, f))
        path_to_df_dict[f] = df

    return new_dfs, path_to_df_dict


def classify_per_table_schema(dataframes):
    """
    Two schemas are the same if they have the exact same number and type of columns
    :param dataframes:
    :return:
    """
    schema_to_dataframes = defaultdict(list)
    for df, path in dataframes:
        the_hashes = [hash(el) for el in df.columns]
        schema_id = sum(the_hashes)
        schema_to_dataframes[schema_id].append((df, path))
    return schema_to_dataframes  # , schema_id_info


def power_set(iterable, size_range):
    "power_set([1,2,3], (0, 3)) --> () (1,) (2,) (3,) (1,2) (1,3) (2,3) (1,2,3)"
    s = list(iterable)
    return chain.from_iterable(combinations(s, r) for r in range(size_range[0], size_range[1] + 1))


def find_candidate_keys(df, sampling=False, max_num_attr_in_composite_key=2, uniqueness_threshold=0.9):
    candidate_keys = []

    # infer and convert types (originally all columns have 'object' type)

    # drop float-type columns
    df = df.select_dtypes(exclude=['float'])
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

    # Find all candidate keys that have the same/similar maximum strength above some threshold
    max_strength = 0
    epsilon = 1e-8
    for key in map(list, filter(None, possible_keys)):
        # strength of a key = the number of distinct key values in the dataset divided by the number of rows
        num_groups = sample.groupby(key).ngroups
        strength = num_groups / sample_size

        if strength < uniqueness_threshold:
            continue

        if abs(strength - max_strength) < epsilon:
            candidate_keys.append(tuple(key))
        elif strength > max_strength:
            candidate_keys = [tuple(key)]
            max_strength = strength

    return candidate_keys


def build_inverted_index(dfs, candidate_key_size=2, uniqueness_threshold=0.9):
    candidate_key_to_inverted_index = defaultdict(lambda: defaultdict(list))

    total_find_candidate_keys_time = 0.0
    total_create_inverted_index_time = 0.0

    view_to_candidate_keys_dict = {}

    if len(dfs) <= 1:
        return candidate_key_to_inverted_index, view_to_candidate_keys_dict, \
            total_find_candidate_keys_time

    for df, path in dfs:

        start_time = time.time()

        candidate_keys = find_candidate_keys(df, sampling=False,
                                             max_num_attr_in_composite_key=candidate_key_size,
                                             uniqueness_threshold=uniqueness_threshold)
        total_find_candidate_keys_time += time.time() - start_time

        start_time = time.time()

        view_to_candidate_keys_dict[path] = candidate_keys

        # if we didn't find any key (ex. all the columns are float), then we don't classify any
        # contradictory or complementary groups
        if len(candidate_keys) == 0:
            continue

        for candidate_key in candidate_keys:

            key_values = df[list(candidate_key)].values.tolist()

            for i, key_value in enumerate(key_values):
                candidate_key_to_inverted_index[candidate_key][tuple(key_value)].append((df, path, i))

        total_create_inverted_index_time += time.time() - start_time

    return candidate_key_to_inverted_index, view_to_candidate_keys_dict, \
        total_find_candidate_keys_time


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

    return row_strs


def highlight_diff(df1, df2, color='pink'):
    # Define html attribute
    attr = 'background-color: {}'.format(color)
    # Where df1 != df2 set attribute
    return pd.DataFrame(np.where(df1.ne(df2), attr, ''), index=df1.index, columns=df1.columns)


def highlight_cols(s, color='lightgreen'):
    return 'background-color: %s' % color
