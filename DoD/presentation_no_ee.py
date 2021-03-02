from DoD import view_4c_analysis_baseline as v4c
from DoD import material_view_analysis as mva
from tqdm import tqdm
import random
from enum import Enum
import glob
import pandas as pd
import pprint
import numpy as np

from DoD.colors import Colors

import server_config as config


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


class Mode(Enum):
    manual = 1,
    random = 2,
    optimal = 3


if __name__ == '__main__':

    #################################CONFIG#####################################
    dir_path = "./mit_id/"
    # top-k views
    top_k = 10
    # epsilon-greedy
    epsilon = 0.1
    # max size of candidate (composite) key
    candidate_key_size = 2
    # sample size of contradictory and complementary rows to present
    sample_size = 5

    mode = Mode.optimal

    max_num_interactions = 1000

    num_runs = 10
    ############################################################################

    pd.set_option('display.max_columns', None)
    pd.set_option('display.max_rows', None)
    pd.set_option('display.max_colwidth', None)
    pd.set_option('display.width', None)  # or 199

    msg_vspec = """
                    ######################################################################################################################
                    #                                              View Presentation                                                     #
                    #                    Goal: Help users find their preferred view among all generated views                            #                 
                    # 1. Run 4C algorithm that classifies the views into 4 categories:                                                   #
                    #    Compatible, Contained, Contradictory, Complementary                                                             #
                    # 2. Remove duplicates in compatible views and keep the view with the largest cardinality in contained views         #
                    # 3. Users choose the candidate key and its respective contradictory and complementary rows                          #
                    # 4. Exploitation vs exploration: exploit the knowledge based on user's previous selections                          #
                    #    and explore other options occasionally                                                                          #
                    # 5. Rank the views based on user's preference by keeping an inverted index from each row to the views containing it #                                                             #
                    ######################################################################################################################
                  """
    print(msg_vspec)

    # Run 4C
    print(Colors.CBOLD + "--------------------------------------------------------------------------" + Colors.CEND)
    print("Running 4C...")

    results, views_by_schema_dict = v4c.main(dir_path, candidate_key_size)

    # TODO: don't separate by schemas for now
    compatible_groups = []
    contained_groups = []
    complementary_groups = []
    contradictory_groups = []
    all_pair_contr_compl = {}

    for k, v in results.items():
        compatible_groups = compatible_groups + v['compatible']
        contained_groups = contained_groups + v['contained']
        complementary_groups = complementary_groups + v['complementary']
        contradictory_groups = contradictory_groups + v['contradictory']
        all_pair_contr_compl.update(v['all_pair_contr_compl'])

    # print(compatible_groups)
    # print(contained_groups)

    print()
    print(Colors.CBOLD + "--------------------------------------------------------------------------" + Colors.CEND)

    csv_files = glob.glob(dir_path + "/view_*")
    print("Number of views: ", len(csv_files))

    # Only keep the view with the largest cardinality in contained group
    largest_contained_views = set()
    for contained_group in contained_groups:
        max_size = 0
        largest_view = contained_group[0]
        for view in contained_group:
            if len(view) > max_size:
                max_size = len(view)
                largest_view = view
        largest_contained_views.add(largest_view)

    # print(largest_contained_views)

    # has_compatible_view_been_added = [False] * len(compatible_groups)

    view_files = set()

    for f in csv_files:
        # already_added = False
        # for i, compatible_group in enumerate(compatible_groups):
        #     if f in compatible_group:
        #         if has_compatible_view_been_added[i]:
        #             already_added = True
        #         else:
        #             has_compatible_view_been_added[i] = True
        add = True
        # Remove duplicates in compatible groups, only keep the first view in each group
        for compatible_group in compatible_groups:
            if f in compatible_group:
                if f != compatible_group[0]:
                    add = False
                    break
        for contained_group in contained_groups:
            if f in contained_group:
                if f not in largest_contained_views:
                    add = False
                    break

        if add:
            view_files.add(f)

    print("After processing compatible and contained groups: ", len(view_files))

    print(Colors.CBOLD + "--------------------------------------------------------------------------" + Colors.CEND)
    print("Processing complementary and contradictory views...")
    all_pair_contr_compl_new = {}

    # key: contradictory or complementary row, value: set of views containing the row
    row_to_path_dict = {}


    def add_to_row_to_path_dict(row_strs, path):
        for row in row_strs:
            if row not in row_to_path_dict.keys():
                row_to_path_dict[row] = {path}
            else:
                row_to_path_dict[row].add(path)


    import time

    contr_or_compl_views = set()

    for path, result in tqdm(all_pair_contr_compl.items()):
        path1 = path[0]
        path2 = path[1]

        contr_or_compl_views.add(path1)
        contr_or_compl_views.add(path2)

        # print("processing " + path1 + " " + path2)

        if not (path1 in view_files and path2 in view_files):
            continue

        # print("----IO----")
        # start = time.time()
        df1 = pd.read_csv(path1, encoding='latin1', thousands=',')
        df1 = mva.curate_view(df1)
        df1 = v4c.normalize(df1)
        # df1 = df1.sort_index(axis=1)
        df2 = pd.read_csv(path2, encoding='latin1', thousands=',')
        df2 = mva.curate_view(df2)
        df2 = v4c.normalize(df2)
        # df2 = df2.sort_index(axis=1)
        # print(time.time() - start)

        all_pair_contr_compl_new[path] = {}

        # print("---loop----")
        # loopstart = time.time()

        get_row_from_key_time = 0
        row_df_to_string_time = 0
        add_to_row_to_path_dict_time = 0

        for candidate_key_tuple, result_list in result.items():

            complementary_keys1 = result_list[0]
            complementary_keys2 = result_list[1]
            contradictory_keys = result_list[2]

            if len(contradictory_keys) > 0:

                all_pair_contr_compl_new[path][candidate_key_tuple] = ["contradictory"]

                # a list containing candidate key names
                candidate_key = list(candidate_key_tuple)
                # a list of tuples, each tuple corresponds to the contradictory key values
                key_values = list(contradictory_keys)
                if len(key_values) > sample_size:
                    key_values = random.sample(key_values, k=sample_size)

                # there could be multiple contradictions existing in a pair of views
                for key_value in key_values:
                    # select row based on multiple composite keys
                    # start = time.time()
                    row1_df = get_row_from_key(df1, candidate_key, key_value)
                    row2_df = get_row_from_key(df2, candidate_key, key_value)
                    # get_row_from_key_time += (time.time() - start)

                    all_pair_contr_compl_new[path][candidate_key_tuple].append((row1_df, row2_df))

                    # start = time.time()
                    row1_strs = row_df_to_string(row1_df)
                    row2_strs = row_df_to_string(row2_df)
                    # row_df_to_string_time += (time.time() - start)

                    # start = time.time()
                    add_to_row_to_path_dict(row1_strs, path1)
                    add_to_row_to_path_dict(row2_strs, path2)
                    # add_to_row_to_path_dict_time += (time.time() - start)

            if len(contradictory_keys) == 0:

                all_pair_contr_compl_new[path][candidate_key_tuple] = ["complementary"]

                candidate_key = list(candidate_key_tuple)
                key_values1 = list(complementary_keys1)
                key_values2 = list(complementary_keys2)
                if len(key_values1) > sample_size:
                    key_values1 = random.sample(key_values1, k=sample_size)
                if len(key_values2) > sample_size:
                    key_values2 = random.sample(key_values2, k=sample_size)

                row1_dfs = []
                for key_value in key_values1:
                    # start = time.time()
                    row1_df = get_row_from_key(df1, candidate_key, key_value)
                    # get_row_from_key_time += (time.time() - start)
                    # print(df1, candidate_key, key_value)
                    # print(row1_df)
                    row1_dfs.append(row1_df)

                    # start = time.time()
                    row1_strs = row_df_to_string(row1_df)
                    # row_df_to_string_time += (time.time() - start)

                    # start = time.time()
                    add_to_row_to_path_dict(row1_strs, path1)
                    # add_to_row_to_path_dict_time += (time.time() - start)

                row2_dfs = []
                for key_value in key_values2:
                    # start = time.time()
                    row2_df = get_row_from_key(df2, candidate_key, key_value)
                    # get_row_from_key_time += (time.time() - start)
                    # print(df2, candidate_key, key_value)
                    # print(row2_df)
                    row2_dfs.append(row2_df)

                    # start = time.time()
                    row2_strs = row_df_to_string(row2_df)
                    # row_df_to_string_time += (time.time() - start)

                    # start = time.time()
                    add_to_row_to_path_dict(row2_strs, path2)
                    # add_to_row_to_path_dict_time += (time.time() - start)

                all_pair_contr_compl_new[path][candidate_key_tuple].append((row1_dfs, row2_dfs))

        # print(time.time() - loopstart)
        # print("get_row_from_key_time: " + str(get_row_from_key_time))
        # print("row_df_to_string_time: " + str(row_df_to_string_time))
        # print("add_to_row_to_path_dict_time: " + str(add_to_row_to_path_dict_time))

    non_contr_or_compl_views = view_files - contr_or_compl_views
    non_contr_or_compl_views_df = []
    # print(non_contr_or_compl_views)
    for path in non_contr_or_compl_views:
        df = pd.read_csv(path, encoding='latin1', thousands=',')
        df = mva.curate_view(df)
        df = v4c.normalize(df)

        row_strs = row_df_to_string(df)
        add_to_row_to_path_dict(row_strs, path)

        sample_df = df
        if len(df) > sample_size:
            sample_df = df.sample(n=sample_size)
        non_contr_or_compl_views_df.append((path, sample_df))


    def sort_view_by_scores(view_rank):
        sorted_view_rank = [(view, score) for view, score in
                            sorted(view_rank.items(), key=lambda item: item[1], reverse=True)]
        return sorted_view_rank


    def get_view_rank_with_ties(sorted_view_rank, view):
        # Same rank for ties
        res = {}
        prev = None
        for i, (v, score) in enumerate(sorted_view_rank):
            if score != prev:
                place, prev = i + 1, score
            res[v] = place

        if view in res.keys():
            rank = res[ground_truth_path]
            return rank
        return None


    def print_option(option_num, df):
        print(Colors.CGREENBG2 + "Option " + str(option_num) + Colors.CEND)
        print(df)


    ground_truth_rank = np.empty((num_runs, len(all_pair_contr_compl_new.keys()) + len(non_contr_or_compl_views_df)),
                                 dtype=int)

    sum_num_interactions = 0

    # ground_truth_path = "./mit_id/view_3"
    # fact_bank_df = None
    # optimal_candidate_key = ["Building Room", "Building Name"]
    # if mode == Mode.optimal:
    #     print("Ground truth view: " + ground_truth_path)
    #     fact_bank_df = pd.read_csv(ground_truth_path, encoding='latin1', thousands=',')
    #     fact_bank_df = mva.curate_view(fact_bank_df)
    #     fact_bank_df = v4c.normalize(fact_bank_df)

    for run in range(num_runs):

        print("Run " + str(run))

        ground_truth_path = random.choice(list(view_files))
        fact_bank_df = None
        optimal_candidate_key = ["Building Room", "Building Name"]
        if mode == Mode.optimal:
            print("Ground truth view: " + ground_truth_path)
            fact_bank_df = pd.read_csv(ground_truth_path, encoding='latin1', thousands=',')
            fact_bank_df = mva.curate_view(fact_bank_df)
            fact_bank_df = v4c.normalize(fact_bank_df)

        # Initialize ranking model
        key_rank = {}
        row_rank = row_to_path_dict.copy()
        for row, path in row_rank.items():
            row_rank[row] = 0
        view_rank = {}
        for path in view_files:
            view_rank[path] = 0

        paths = list(all_pair_contr_compl_new.keys())
        random.shuffle(paths)

        num_interactions = 0
        loop_count = 0

        non_contr_or_compl_views_df_copy = non_contr_or_compl_views_df.copy()

        # for path in paths:
        while num_interactions < max_num_interactions:

            if len(paths) <= 0 and len(non_contr_or_compl_views_df_copy) <= 0:
                break

            path = None
            single_view = None
            if len(paths) <= 0:
                single_view = non_contr_or_compl_views_df_copy.pop()
            elif len(non_contr_or_compl_views_df_copy) <= 0:
                path = paths.pop()
            else:
                p = random.random()
                if p < 0.5:
                    path = paths.pop()
                else:
                    single_view = non_contr_or_compl_views_df_copy.pop()

            print(
                Colors.CBOLD + "--------------------------------------------------------------------------" +
                Colors.CEND)

            count = 0
            option_dict = {}

            if single_view != None:
                # present the single view
                count += 1
                path, sample_df = single_view
                print(Colors.CBLUEBG2 + path + Colors.CEND)
                print_option(count, sample_df)
                option_dict[count] = (None, [sample_df], path)
            else:
                path1 = path[0]
                path2 = path[1]

                print(Colors.CBLUEBG2 + path1 + " - " + path2 + Colors.CEND)

                candidate_key_dict = all_pair_contr_compl_new[path]

                for candidate_key_tuple, contr_or_compl_df_list in candidate_key_dict.items():

                    if candidate_key_tuple not in key_rank.keys():
                        key_rank[candidate_key_tuple] = 0

                    print("Candidate key " + Colors.CREDBG2 + str(candidate_key_tuple) + Colors.CEND + " is "
                          + Colors.CVIOLETBG2 + contr_or_compl_df_list[0] + Colors.CEND)

                    if contr_or_compl_df_list[0] == "contradictory":
                        # print(contr_or_compl_df_list)
                        row1_dfs = []
                        row2_dfs = []

                        skip_this_pair = False
                        preferred_view_set = set()

                        for row_tuple in contr_or_compl_df_list[1:]:
                            row1_dfs.append(row_tuple[0])
                            row2_dfs.append(row_tuple[1])

                        # concatenate all contradictory rows in both side
                        if len(row1_dfs) > 0 and len(row2_dfs) > 0:
                            contradictory_rows1 = pd.concat(row1_dfs)
                            count += 1
                            print_option(count, contradictory_rows1)
                            option_dict[count] = (candidate_key_tuple, row1_dfs, path1)

                            contradictory_rows2 = pd.concat(row2_dfs)
                            count += 1
                            print_option(count, contradictory_rows2)
                            option_dict[count] = (candidate_key_tuple, row2_dfs, path2)

                    if contr_or_compl_df_list[0] == "complementary":
                        # TODO: epsilon greedy for complementary rows?
                        #  But they are not really "choose one over the other" relationship

                        # concatenate all complementary (non-intersecting) rows in both side
                        complementary_df_tuple = contr_or_compl_df_list[1]
                        count += 1
                        complementary_part1 = pd.concat(complementary_df_tuple[0])
                        print_option(count, complementary_part1)
                        option_dict[count] = (candidate_key_tuple, complementary_df_tuple[0], path1)

                        count += 1
                        complementary_part2 = pd.concat(complementary_df_tuple[1])
                        print_option(count, complementary_part2)
                        option_dict[count] = (candidate_key_tuple, complementary_df_tuple[1], path2)

            if len(option_dict) > 0:

                num_interactions += 1

                option_picked = 0

                if mode == Mode.optimal:
                    max_intersection_with_fact_back = 0
                    for option, values in option_dict.items():
                        candidate_key = values[0]
                        # if set(candidate_key) == set(optimal_candidate_key):
                        row_dfs = values[1]
                        concat_row_df = pd.concat(row_dfs)
                        intersection = pd.merge(left=concat_row_df, right=fact_bank_df, on=None)  # default to
                        # intersection
                        if len(intersection) > max_intersection_with_fact_back:
                            # Always selection the option that's more consistent with the fact bank
                            # if there's no intersection, then skip this option (select 0)
                            option_picked = option
                            max_intersection_with_fact_back = len(intersection)
                            # print(str(max_intersection_with_fact_back) + " " + str(option_picked))
                    print(Colors.CGREYBG + "Select option (or 0 if no preferred option): " + Colors.CEND)
                    print("Optimal option = " + str(option_picked))

                elif mode == Mode.random:
                    option_picked = random.choice(list(option_dict.keys()))
                    print(Colors.CGREYBG + "Select option (or 0 if no preferred option): " + Colors.CEND)
                    print("Random option = " + str(option_picked))

                else:
                    option_picked = input(
                        Colors.CGREYBG + "Select option (or 0 if no preferred option): " + Colors.CEND)

                    if option_picked == "":
                        break

                    while not (option_picked.isdigit() and
                               (int(option_picked) in option_dict.keys() or int(option_picked) == 0)):
                        option_picked = input(
                            Colors.CGREYBG + "Select option (or 0 if no preferred option): " + Colors.CEND)

                    option_picked = int(option_picked)

                if option_picked != 0:
                    candidate_key_picked = option_dict[option_picked][0]
                    if candidate_key_picked != None:
                        key_rank[candidate_key_picked] += 1

                    # TODO： Add score for any view containing the contradictory or complementary row selected
                    views_to_add_score = set()
                    rows_picked = option_dict[option_picked][1]
                    for row_df in rows_picked:
                        row_strs = row_df_to_string(row_df)

                        for row_str in row_strs:
                            if row_str in row_to_path_dict.keys():
                                paths_containing_row = row_to_path_dict[row_str]
                                for path in paths_containing_row:
                                    views_to_add_score.add(path)

                            if row_str in row_rank.keys():
                                row_rank[row_str] += 1

                    for path in views_to_add_score:
                        view_rank[path] += 1

            print(Colors.CBEIGEBG + "View rank" + Colors.CEND)
            sorted_view_rank = sort_view_by_scores(view_rank)
            pprint.pprint(sorted_view_rank)

            if mode == Mode.optimal or mode == Mode.random:
                # sorted_view_rank = sort_view_by_scores(view_rank)
                rank = get_view_rank_with_ties(sorted_view_rank, ground_truth_path)
                # print("rank = " + str(rank))
                if rank != None:
                    ground_truth_rank[run][loop_count] = rank
                else:
                    print("ERROR!!!")
                    exit()

            loop_count += 1

        print(Colors.CBOLD + "--------------------------------------------------------------------------" + Colors.CEND)
        # print(Colors.CBEIGEBG + "Key rank" + Colors.CEND)
        # pprint.pprint(key_rank)
        # print(Colors.CBEIGEBG + "Row rank" + Colors.CEND)
        # pprint.pprint(row_rank)
        print(Colors.CREDBG2 + "Final Top-" + str(top_k) + " views" + Colors.CEND)
        sorted_view_rank = sort_view_by_scores(view_rank)
        pprint.pprint(sorted_view_rank[:top_k])
        print("Number of interactions = " + str(num_interactions))
        sum_num_interactions += num_interactions

        if mode == Mode.optimal or mode == Mode.random:
            rank = get_view_rank_with_ties(sorted_view_rank, ground_truth_path)
            if rank != None:
                print("Ground truth view " + ground_truth_path + " is top-" + str(rank))
                print(
                    Colors.CBOLD + "--------------------------------------------------------------------------" +
                    Colors.CEND)  # for i in range(len(sorted_view_rank)):
            #     view, score = sorted_view_rank[i]
            #     if ground_truth_path == view:
            #         print("Ground truth view is top-" + str(i+1))

    # avg_ground_truth_rank = np.mean(ground_truth_rank, axis=0)

    print("Average number of interactions = " + str(sum_num_interactions / num_runs))

    import matplotlib.pyplot as plt

    plt.rcParams['figure.figsize'] = [12, 8]
    plt.rcParams['figure.dpi'] = 200

    # x_axis = np.linspace(1, max_num_interactions, num=max_num_interactions)
    # print(ground_truth_rank)
    # print(ground_truth_rank.shape)
    # fig, ax = plt.subplots()

    plt.boxplot(ground_truth_rank[:, ::2])
    locs, labels = plt.xticks()
    # print(locs)
    # print(labels)
    # ax.set_xticks()
    plt.xticks(ticks=locs, labels=np.arange(1, ground_truth_rank.shape[1] + 1, step=2))
    plt.show()
