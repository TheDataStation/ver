import math

from DoD import view_4c_analysis_baseline as v4c
from DoD import material_view_analysis as mva
from DoD.colors import Colors
from tqdm import tqdm
import random
from enum import Enum
import glob
import pandas as pd
import pprint
import numpy as np
from collections import defaultdict
from scipy.stats import rankdata
from collections import defaultdict, namedtuple

pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)
pd.set_option('display.max_colwidth', None)
pd.set_option('display.width', None)  # or 199


class Mode(Enum):
    manual = 1,
    random = 2,
    optimal = 3


def prune_compatible_views(views, compatible_groups):
    pruned_views = set()
    for f in views:
        add = True
        # Remove duplicates in compatible groups, only keep the first view in each group
        for compatible_group in compatible_groups:
            if f in compatible_group:
                if f != compatible_group[0]:
                    add = False
                    break
        if add:
            pruned_views.add(f)
    return pruned_views


def prune_contained_views(views, contained_groups):
    # Only keep the view with the largest cardinality in contained group
    largest_contained_views = set()
    for contained_group in contained_groups:
        max_size = 0
        largest_view = contained_group[0]
        for view in contained_group:
            df = pd.read_csv(view, encoding='latin1', thousands=',')
            df = mva.curate_view(df)
            df = v4c.normalize(df)

            if len(df) > max_size:
                max_size = len(df)
                largest_view = view
        largest_contained_views.add(largest_view)

    # print(largest_contained_views)

    pruned_views = set()
    for f in views:
        add = True
        for contained_group in contained_groups:
            if f in contained_group:
                if f not in largest_contained_views:
                    add = False
                    break

        if add:
            pruned_views.add(f)
    return pruned_views


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


def preprocess(view_files, all_pair_contr_compl, sample_size):
    all_pair_contr_compl_new = {}

    # key: contradictory or complementary row, value: set of views containing the row
    row_to_path_dict = {}

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
                    add_to_row_to_path_dict(row_to_path_dict, row1_strs, path1)
                    add_to_row_to_path_dict(row_to_path_dict, row2_strs, path2)
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
                    add_to_row_to_path_dict(row_to_path_dict, row1_strs, path1)
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
                    add_to_row_to_path_dict(row_to_path_dict, row2_strs, path2)
                    # add_to_row_to_path_dict_time += (time.time() - start)

                all_pair_contr_compl_new[path][candidate_key_tuple].append((row1_dfs, row2_dfs))

        # print(time.time() - loopstart)
        # print("get_row_from_key_time: " + str(get_row_from_key_time))
        # print("row_df_to_string_time: " + str(row_df_to_string_time))
        # print("add_to_row_to_path_dict_time: " + str(add_to_row_to_path_dict_time))

    non_contr_or_compl_view_files = view_files - contr_or_compl_views
    non_contr_or_compl_views = []
    # print(non_contr_or_compl_views)
    for path in non_contr_or_compl_view_files:
        df = pd.read_csv(path, encoding='latin1', thousands=',')
        df = mva.curate_view_not_dropna(df)
        df = v4c.normalize(df)
        # print(df)
        row_strs = row_df_to_string(df)
        add_to_row_to_path_dict(row_to_path_dict, row_strs, path)

        # sample_df = df
        # if len(df) > sample_size:
        #     sample_df = df.sample(n=sample_size)
        non_contr_or_compl_views.append((path, df))

    return all_pair_contr_compl_new, non_contr_or_compl_views, row_to_path_dict


def sort_view_by_scores(view_rank):
    sorted_view_rank = [(view, score) for view, score in
                        sorted(view_rank.items(), key=lambda item: item[1], reverse=True)]
    return sorted_view_rank


def get_view_rank_with_ties(view_scores, view):
    # add negative sign in order for rankdata to rank in reverse direction
    scores = [-score for v, score in view_scores.items()]
    views = [v for v, score in view_scores.items()]

    # The average of the ranks that would have been assigned to all the tied values is assigned to each value.
    ranks = list(rankdata(scores, method='average'))

    for i in range(len(views)):
        if views[i] == view:
            rank = ranks[i]
            return rank
    return None


def pick_from_top_k_views(view_rank, view_to_view_pairs_dict, non_contr_or_compl_views_df, k):
    path = None
    single_view_list = []

    sorted_view_rank = sort_view_by_scores(view_rank)

    top_k_views = sorted_view_rank[:k]
    # if k > len(sorted_view_rank):
    #     k = len(sorted_view_rank)
    # for i in range(k):
    #     top_k_views.append(sorted_view_rank[i][0])
    # print(top_k_views)

    p = random.random()

    # TODO: in some probability, either pick singleton views or pick a C/C pair from top-k

    if p < 0.3 and len(non_contr_or_compl_views_df) > 0:
        for view, score in top_k_views:
            for view_df in non_contr_or_compl_views_df:
                if view == view_df[0] and score > 0:
                    single_view_list.append(view_df)
    if len(single_view_list) > 0:
        return path, single_view_list

    # if p > 0.5 or there is no single view present in top-k
    for view1, score1 in top_k_views:
        for view2, score2 in top_k_views:
            if view1 != view2 and score1 + score2 > 0:
                if view1 in view_to_view_pairs_dict.keys():
                    if view2 in view_to_view_pairs_dict[view1]:
                        path = (view1, view2)
                        return path, single_view_list

    return path, single_view_list


def highlight_diff(df1, df2, color='pink'):
    # Define html attribute
    attr = 'background-color: {}'.format(color)
    # Where df1 != df2 set attribute
    return pd.DataFrame(np.where(df1.ne(df2), attr, ''), index=df1.index, columns=df1.columns)


def highlight_cols(s, color='lightgreen'):
    return 'background-color: %s' % color


def present(view_files, contr_or_compl_view_pairs, non_contr_or_compl_views, row_to_path_dict, top_k, epsilon,
            max_num_interactions, sample_size):
    from ipywidgets import Output, ToggleButtons, interact, fixed
    from IPython.display import display, clear_output, HTML

    # import asyncio
    # def wait_for_change(widget, value):
    #     future = asyncio.Future()
    #
    #     def on_click(change):
    #         if change['new'] == "Skip":
    #             future.set_result(0)
    #         elif change['new'] == "Stop":
    #             future.set_result(-1)
    #         else:
    #             future.set_result(int(change['new']))
    #         widget.unobserve(on_click, value)
    #
    #     widget.observe(on_click, value)
    #     return future

    out = Output()

    @out.capture()
    def print_option(option_num, html):
        print(Colors.CWHITEBG + "Option " + str(option_num) + Colors.CEND)
        display(HTML(html))

    def print_line():
        print(
            Colors.CBOLD + "--------------------------------------------------------------------------" +
            Colors.CEND)

    # Initialize ranking model
    key_rank = {}
    row_rank = row_to_path_dict.copy()
    for row, path in row_rank.items():
        row_rank[row] = 0
    view_rank = {}
    for path in view_files:
        view_rank[path] = 0

    # TODO: dynamic exploration / exploitation
    paths = list(contr_or_compl_view_pairs.keys())
    random.shuffle(paths)

    # Explore unexplored views first
    all_distinct_view_pairs = set()
    distinct_views = set()
    for path in paths:
        path1 = path[0]
        path2 = path[1]
        if path1 not in distinct_views and path2 not in distinct_views:
            all_distinct_view_pairs.add(path)
            distinct_views.add(path1)
            distinct_views.add(path2)
    other_non_distinct_view_pairs = set(paths) - set(all_distinct_view_pairs)

    view_to_view_pairs_dict = defaultdict(list)
    for path in paths:
        path1 = path[0]
        path2 = path[1]
        view_to_view_pairs_dict[path1].append(path2)
        view_to_view_pairs_dict[path2].append(path1)

    # print(len(paths))
    # print(len(all_distinct_view_pairs))
    # print(len(other_non_distinct_view_pairs))

    non_contr_or_compl_views_copy = non_contr_or_compl_views.copy()

    # async def f():
    num_interactions = 0
    while num_interactions < max_num_interactions:

        path = None
        single_view_list = []

        clear_output()
        out.clear_output()

        if len(view_to_view_pairs_dict) <= 0 and len(non_contr_or_compl_views_copy) <= 0:
            # we have explored all the contradictory / complementary view pairs and single views at least once
            with out:
                print("You have explored all views")
            break

        sorted_view_rank = sort_view_by_scores(view_rank)
        with out:
            print(Colors.CBEIGEBG + "Current view scores" + Colors.CEND)
            pprint.pprint(sorted_view_rank)
        # display(out)

        # Explore unexplored views first
        # if len(all_distinct_view_pairs) > 0 or len(
        #         non_contr_or_compl_views_copy) > 0:  # and num_interactions < max_num_interactions / 2:
        #
        #     if len(all_distinct_view_pairs) <= 0:
        #         single_view = non_contr_or_compl_views_copy.pop()
        #         single_view_list.append(single_view)
        #     elif len(non_contr_or_compl_views_copy) <= 0:
        #         path = all_distinct_view_pairs.pop()
        #     else:
        #         p = random.random()
        #         if p < 0.5:
        #             path = all_distinct_view_pairs.pop()
        #         else:
        #             single_view = non_contr_or_compl_views_copy.pop()
        #             single_view_list.append(single_view)
        # else:

        # Epsilon-greedy: Pick the best available pair from top-k views for users to choose(exploitation),
        # or pick a random pair (exploration)
        p = random.random()
        if p > epsilon:
            path, single_view_list = pick_from_top_k_views(view_rank, view_to_view_pairs_dict,
                                                           non_contr_or_compl_views, top_k)

        # path = None -> all pairs from current top-k views have been explored
        if (path == None and len(single_view_list) == 0) or p <= epsilon:

            if len(view_to_view_pairs_dict) == 0:
                single_view = non_contr_or_compl_views_copy.pop()
                single_view_list.append(single_view)
            elif len(non_contr_or_compl_views_copy) == 0:
                view1, pair_list = random.choice(list(view_to_view_pairs_dict.items()))
                view2 = random.choice(pair_list)
                path = (view1, view2)
            else:
                p2 = random.random()

                if p2 < 0.5:
                    single_view = non_contr_or_compl_views_copy.pop()
                    single_view_list.append(single_view)
                else:
                    view1, pair_list = random.choice(list(view_to_view_pairs_dict.items()))
                    # pprint.pprint(view_to_view_pairs_dict)
                    # print(view1)
                    view2 = random.choice(pair_list)
                    path = (view1, view2)

        count = 0
        option_dict = {}

        if len(single_view_list) > 0:
            with out:
                print()
                print_line()
                print("Presenting singleton views")
            # present the single views
            for single_view in single_view_list:
                count += 1
                path, df = single_view
                sample_df = df
                if len(df) > sample_size:
                    sample_df = df.sample(n=sample_size)
                with out:
                    print(Colors.CBLUEBG + path + Colors.CEND)
                print_option(count, sample_df.to_html())
                option_dict[count] = (None, [sample_df], path)

                if single_view in non_contr_or_compl_views_copy:
                    non_contr_or_compl_views_copy.remove(single_view)
        else:
            path1 = path[0]
            path2 = path[1]
            if path not in contr_or_compl_view_pairs.keys():
                path = (path2, path1)
                path1 = path[0]
                path2 = path[1]

            view_to_view_pairs_dict[path1].remove(path2)
            view_to_view_pairs_dict[path2].remove(path1)

            if len(view_to_view_pairs_dict[path1]) == 0:
                del view_to_view_pairs_dict[path1]
                # print("deleted " + path1)
            if len(view_to_view_pairs_dict[path2]) == 0:
                del view_to_view_pairs_dict[path2]
                # print("deleted " + path2)

            # pprint.pprint(view_to_view_pairs_dict)

            with out:
                print()
                print(Colors.CBLUEBG + path1 + " - " + path2 + Colors.CEND)

            candidate_key_dict = contr_or_compl_view_pairs[path]

            # exploitation vs exploration
            # TODO:
            #  Epsilon-greedy:
            #  If the user has selected the a candidate key (n times) more frequently than the others,
            #  this means they are pretty confident about their choices, so we don't bother showing them other keys
            #  again.
            #  In epsilon probability, we still show the user all candidate keys in case they made a mistake or
            #  want to explore other keys

            n = 2

            p = random.random()

            best_key = None
            if p > epsilon:
                max_score = -1
                for candidate_key_tuple in candidate_key_dict.keys():
                    if candidate_key_tuple in key_rank.keys():
                        if key_rank[candidate_key_tuple] > max_score:
                            best_key = candidate_key_tuple
                            max_score = key_rank[candidate_key_tuple]
                if best_key != None:
                    sum_scores = 0
                    other_keys = []
                    for key in candidate_key_dict.keys():
                        if key != best_key and key in key_rank.keys():
                            sum_scores += key_rank[key]
                            other_keys.append(key)
                    # Exclude other keys because the best key was selected much more frequently than the others
                    if max_score > n * sum_scores and max_score - sum_scores > n:
                        for key in other_keys:
                            del candidate_key_dict[key]
                    else:
                        best_key = None

            for candidate_key_tuple, contr_or_compl_df_list in candidate_key_dict.items():

                if candidate_key_tuple not in key_rank.keys():
                    key_rank[candidate_key_tuple] = 0

                with out:
                    print_line()
                    if contr_or_compl_df_list[0] == "contradictory":
                        print(
                            "Candidate key " + Colors.CBOLD + str(list(candidate_key_tuple)) + Colors.CEND + " yields "
                            + Colors.CREDBG + contr_or_compl_df_list[0] + Colors.CEND + " values")
                    else:
                        print(
                            "Candidate key " + Colors.CBOLD + str(list(candidate_key_tuple)) + Colors.CEND + " yields "
                            + Colors.CGREENBG + contr_or_compl_df_list[0] + Colors.CEND + " values")

                if contr_or_compl_df_list[0] == "contradictory":
                    # print(contr_or_compl_df_list)
                    row1_dfs = []
                    row2_dfs = []

                    skip_this_pair = False
                    preferred_view_set = set()

                    for row_tuple in contr_or_compl_df_list[1:]:

                        # TODO: epsilon greedy
                        #  If the user selected one contradictory row (n times) more frequently over the other,
                        #  skip this contradiction

                        exclude_this_contradiction = False
                        if p > epsilon:
                            row1_strs = row_df_to_string(row_tuple[0])
                            row2_strs = row_df_to_string(row_tuple[1])
                            for row1 in row1_strs:
                                for row2 in row2_strs:
                                    if (row_rank[row1] > n * row_rank[row2] and row_rank[row1] - row_rank[
                                        row2] > n) or \
                                            (row_rank[row2] > n * row_rank[row1] and row_rank[row2] - row_rank[
                                                row1] > n):
                                        preferred_view = path1 if row_rank[row1] > n * row_rank[row2] else path2
                                        preferred_view_set.add(preferred_view)
                                        # exclude this particular contradiction
                                        exclude_this_contradiction = True

                            if exclude_this_contradiction:
                                continue

                        row1_dfs.append(row_tuple[0])
                        row2_dfs.append(row_tuple[1])

                    # concatenate all contradictory rows in both side
                    if len(row1_dfs) > 0 and len(row2_dfs) > 0:

                        contradictory_rows1 = pd.concat(row1_dfs).reset_index(drop=True)
                        contradictory_rows2 = pd.concat(row2_dfs).reset_index(drop=True)

                        html1 = contradictory_rows1.style \
                            .applymap(highlight_cols, subset=pd.IndexSlice[:, list(candidate_key_tuple)],
                                      color='lightyellow') \
                            .apply(highlight_diff, axis=None, df2=contradictory_rows2) \
                            .render()

                        html2 = contradictory_rows2.style \
                            .applymap(highlight_cols, subset=pd.IndexSlice[:, list(candidate_key_tuple)],
                                      color='lightyellow') \
                            .apply(highlight_diff, axis=None, df2=contradictory_rows1) \
                            .render()

                        count += 1
                        print_option(count, html1)
                        option_dict[count] = (candidate_key_tuple, row1_dfs, path1)

                        count += 1
                        print_option(count, html2)
                        option_dict[count] = (candidate_key_tuple, row2_dfs, path2)
                    else:
                        if best_key != None:
                            # If there's already a preferred key
                            if len(preferred_view_set) == 1:
                                # If the user always select all the contradictory rows in one view over the other
                                # then skip this pair
                                preferred_view = preferred_view_set.pop()
                                with out:
                                    print("Skipping this pair...")
                                    print("Automatically selecting preferred view " + preferred_view)
                                view_rank[preferred_view] += 1
                                break
                        else:
                            # Otherwise skip showing this contradiction for the current key
                            with out:
                                print("Automatically skipping all contradictions based on previous selections")

                if contr_or_compl_df_list[0] == "complementary":
                    # TODO: epsilon greedy for complementary rows?
                    #  But they are not really "choose one over the other" relationship

                    # concatenate all complementary (non-intersecting) rows in both side
                    complementary_df_tuple = contr_or_compl_df_list[1]

                    if len(complementary_df_tuple[0]) > 0:
                        complementary_part1 = pd.concat(complementary_df_tuple[0])
                        html1 = complementary_part1.style.applymap(highlight_cols, subset=pd.IndexSlice[:, list(
                            candidate_key_tuple)]).render()

                        count += 1
                        print_option(count, html1)
                        option_dict[count] = (candidate_key_tuple, complementary_df_tuple[0], path1)
                    if len(complementary_df_tuple[1]) > 0:
                        complementary_part2 = pd.concat(complementary_df_tuple[1])
                        html2 = complementary_part2.style.applymap(highlight_cols, subset=pd.IndexSlice[:, list(
                            candidate_key_tuple)]).render()

                        count += 1
                        print_option(count, html2)
                        option_dict[count] = (candidate_key_tuple, complementary_df_tuple[1], path2)

        if len(option_dict) > 0:

            num_interactions += 1

            # option_list = [str(option) for option in option_dict.keys()]
            # option_list.append("Skip")
            # option_list.append("Stop")
            #
            # toggle_buttons = ToggleButtons(
            #     options=[str(option) for option in option_dict.keys()],
            #     description='Options:',
            #     disabled=False,
            #     button_style='', # 'success', 'info', 'warning', 'danger' or ''
            #     # tooltips=['Description of slow', 'Description of regular', 'Description of fast'],
            # #     icons=['check'] * 3
            # )
            #
            #
            # option_picked = await wait_for_change(toggle_buttons, 'value')
            #
            # clear_output()
            # out.clear_output()
            display(out)
            # display(toggle_buttons)
            #         clear_output()
            #         out.clear_output()

            option_picked = input(
                Colors.CWHITEBG + "Select option (0: skip, empty: stop): " + Colors.CEND)

            if option_picked == "":
                with out:
                    print("Stopped interaction")
                break

            while not (option_picked.isdigit() and
                       (int(option_picked) in option_dict.keys() or int(option_picked) == 0)):
                option_picked = input(
                    Colors.CWHITEBG + "Select option (0: skip, empty: stop): " + Colors.CEND)

            option_picked = int(option_picked)

            if option_picked != 0:
                candidate_key_picked = option_dict[option_picked][0]
                if candidate_key_picked != None:
                    key_rank[candidate_key_picked] += 1

                # TODO： Add score for any view containing the contradictory or complementary row selected
                # views_to_add_score = set()
                rows_picked = option_dict[option_picked][1]
                for row_df in rows_picked:
                    row_strs = row_df_to_string(row_df)

                    for row_str in row_strs:
                        if row_str in row_to_path_dict.keys():
                            paths_containing_row = row_to_path_dict[row_str]
                            for path in paths_containing_row:
                                # views_to_add_score.add(path)
                                view_rank[path] += 1

                        if row_str in row_rank.keys():
                            row_rank[row_str] += 1

                # for path in views_to_add_score:
                #     view_rank[path] += 1

    sorted_view_rank = sort_view_by_scores(view_rank)
    return sorted_view_rank, num_interactions

    #     with out:
    #         print(Colors.CBEIGEBG + "View rank" + Colors.CEND)
    #     sorted_view_rank = sort_view_by_scores(view_rank)
    #     with out:
    #         pprint.pprint(sorted_view_rank)
    #     display(out)
    #     clear_output()
    #     out.clear_output()

    # asyncio.ensure_future(f())
    # clear_output()
    # out.clear_output()
    #
    # sorted_view_rank = sort_view_by_scores(view_rank)
    # with out:
    #     print(Colors.CBOLD + "--------------------------------------------------------------------------" +
    #     Colors.CEND)
    #     # print(Colors.CBEIGEBG + "Key rank" + Colors.CEND)
    #     # pprint.pprint(key_rank)
    #     # print(Colors.CBEIGEBG + "Row rank" + Colors.CEND)
    #     # pprint.pprint(row_rank)
    #     print(Colors.CREDBG + "Final Top-" + str(top_k) + " views" + Colors.CEND)
    #     pprint.pprint(sorted_view_rank[:top_k])
    #     print("Number of interactions = " + str(num_interactions))
    # display(out)


def present_async(view_files, contr_or_compl_view_pairs, non_contr_or_compl_views, row_to_path_dict, top_k, epsilon,
                  max_num_interactions, sample_size):
    from ipywidgets import Output, Button, ToggleButtons, interact, fixed, HBox
    from IPython.display import display, clear_output, HTML

    import asyncio
    # def wait_for_change(widget, value):
    #     future = asyncio.Future()
    #
    #     def on_click(change):
    #         if change['new'] == "Skip":
    #             future.set_result(0)
    #         elif change['new'] == "Stop":
    #             future.set_result(-1)
    #         else:
    #             future.set_result(int(change['new']))
    #         widget.unobserve(on_click, value)
    #
    #     widget.observe(on_click, value)
    #     return future

    def wait_for_change(buttons):
        future = asyncio.Future()

        def getvalue(change):
            future.set_result(change.description)
            for button in buttons:
                button.on_click(getvalue, remove=True)
            # we need to free up the binding to getvalue to avoid an InvalidState error
            # buttons don't support unobserve
            # so use `remove=True`

        for button in buttons:
            button.on_click(getvalue)
        return future

    out = Output()
    skip_button = Button(description="Skip")
    stop_button = Button(description="Stop")

    @out.capture()
    def print_option(option_num, html, buttons):
        # print(Colors.CWHITEBG + "Option " + str(option_num) + Colors.CEND)
        button = Button(description="Option " + str(option_num))
        display(button)
        buttons.append(button)
        display(HTML(html))

    def print_line():
        print(
            Colors.CBOLD + "--------------------------------------------------------------------------" +
            Colors.CEND)

    # Initialize ranking model
    key_rank = {}
    row_rank = row_to_path_dict.copy()
    for row, path in row_rank.items():
        row_rank[row] = 0
    view_rank = {}
    for path in view_files:
        view_rank[path] = 0

    # TODO: dynamic exploration / exploitation
    paths = list(contr_or_compl_view_pairs.keys())
    random.shuffle(paths)

    # Explore unexplored views first
    all_distinct_view_pairs = set()
    distinct_views = set()
    for path in paths:
        path1 = path[0]
        path2 = path[1]
        if path1 not in distinct_views and path2 not in distinct_views:
            all_distinct_view_pairs.add(path)
            distinct_views.add(path1)
            distinct_views.add(path2)
    other_non_distinct_view_pairs = set(paths) - set(all_distinct_view_pairs)

    view_to_view_pairs_dict = defaultdict(list)
    for path in paths:
        path1 = path[0]
        path2 = path[1]
        view_to_view_pairs_dict[path1].append(path2)
        view_to_view_pairs_dict[path2].append(path1)

    # print(len(paths))
    # print(len(all_distinct_view_pairs))
    # print(len(other_non_distinct_view_pairs))

    non_contr_or_compl_views_copy = non_contr_or_compl_views.copy()

    async def f():
        num_interactions = 0
        while num_interactions < max_num_interactions:

            path = None
            single_view_list = []

            # clear_output()
            out.clear_output()

            if len(view_to_view_pairs_dict) <= 0 and len(non_contr_or_compl_views_copy) <= 0:
                # we have explored all the contradictory / complementary view pairs and single views at least once
                with out:
                    print("You have explored all views")
                break

            buttons = [skip_button, stop_button]

            sorted_view_rank = sort_view_by_scores(view_rank)
            with out:
                print(Colors.CBEIGEBG + "Current view scores" + Colors.CEND)
                pprint.pprint(sorted_view_rank)
                display(HBox([skip_button, stop_button]))
            # display(out)

            # Explore unexplored views first
            # if len(all_distinct_view_pairs) > 0 or len(
            #         non_contr_or_compl_views_copy) > 0:  # and num_interactions < max_num_interactions / 2:
            #
            #     if len(all_distinct_view_pairs) <= 0:
            #         single_view = non_contr_or_compl_views_copy.pop()
            #         single_view_list.append(single_view)
            #     elif len(non_contr_or_compl_views_copy) <= 0:
            #         path = all_distinct_view_pairs.pop()
            #     else:
            #         p = random.random()
            #         if p < 0.5:
            #             path = all_distinct_view_pairs.pop()
            #         else:
            #             single_view = non_contr_or_compl_views_copy.pop()
            #             single_view_list.append(single_view)
            # else:

            # Epsilon-greedy: Pick the best available pair from top-k views for users to choose(exploitation),
            # or pick a random pair (exploration)
            p = random.random()
            if p > epsilon:
                path, single_view_list = pick_from_top_k_views(view_rank, view_to_view_pairs_dict,
                                                               non_contr_or_compl_views, top_k)

            # path = None -> all pairs from current top-k views have been explored
            if (path == None and len(single_view_list) == 0) or p <= epsilon:

                if len(view_to_view_pairs_dict) == 0:
                    single_view = non_contr_or_compl_views_copy.pop()
                    single_view_list.append(single_view)
                elif len(non_contr_or_compl_views_copy) == 0:
                    view1, pair_list = random.choice(list(view_to_view_pairs_dict.items()))
                    view2 = random.choice(pair_list)
                    path = (view1, view2)
                else:
                    p2 = random.random()

                    if p2 < 0.5:
                        single_view = non_contr_or_compl_views_copy.pop()
                        single_view_list.append(single_view)
                    else:
                        view1, pair_list = random.choice(list(view_to_view_pairs_dict.items()))
                        # pprint.pprint(view_to_view_pairs_dict)
                        # print(view1)
                        view2 = random.choice(pair_list)
                        path = (view1, view2)

            count = 0
            option_dict = {}

            if len(single_view_list) > 0:
                with out:
                    print()
                    print_line()
                    print("Presenting singleton views")
                # present the single views
                for single_view in single_view_list:
                    count += 1
                    path, df = single_view
                    sample_df = df
                    if len(df) > sample_size:
                        sample_df = df.sample(n=sample_size)
                    with out:
                        print(Colors.CBLUEBG + path + Colors.CEND)
                    print_option(count, sample_df.to_html(), buttons)
                    option_dict[count] = (None, [sample_df], path)

                    if single_view in non_contr_or_compl_views_copy:
                        non_contr_or_compl_views_copy.remove(single_view)
            else:
                path1 = path[0]
                path2 = path[1]
                if path not in contr_or_compl_view_pairs.keys():
                    path = (path2, path1)
                    path1 = path[0]
                    path2 = path[1]

                view_to_view_pairs_dict[path1].remove(path2)
                view_to_view_pairs_dict[path2].remove(path1)

                if len(view_to_view_pairs_dict[path1]) == 0:
                    del view_to_view_pairs_dict[path1]
                    # print("deleted " + path1)
                if len(view_to_view_pairs_dict[path2]) == 0:
                    del view_to_view_pairs_dict[path2]
                    # print("deleted " + path2)

                # pprint.pprint(view_to_view_pairs_dict)

                with out:
                    print()
                    print(Colors.CBLUEBG + path1 + " - " + path2 + Colors.CEND)

                candidate_key_dict = contr_or_compl_view_pairs[path]

                # exploitation vs exploration
                # TODO:
                #  Epsilon-greedy:
                #  If the user has selected the a candidate key (n times) more frequently than the others,
                #  this means they are pretty confident about their choices, so we don't bother showing them other keys
                #  again.
                #  In epsilon probability, we still show the user all candidate keys in case they made a mistake or
                #  want to explore other keys

                n = 2

                p = random.random()

                best_key = None
                if p > epsilon:
                    max_score = -1
                    for candidate_key_tuple in candidate_key_dict.keys():
                        if candidate_key_tuple in key_rank.keys():
                            if key_rank[candidate_key_tuple] > max_score:
                                best_key = candidate_key_tuple
                                max_score = key_rank[candidate_key_tuple]
                    if best_key != None:
                        sum_scores = 0
                        other_keys = []
                        for key in candidate_key_dict.keys():
                            if key != best_key and key in key_rank.keys():
                                sum_scores += key_rank[key]
                                other_keys.append(key)
                        # Exclude other keys because the best key was selected much more frequently than the others
                        if max_score > n * sum_scores and max_score - sum_scores > n:
                            for key in other_keys:
                                del candidate_key_dict[key]
                        else:
                            best_key = None

                for candidate_key_tuple, contr_or_compl_df_list in candidate_key_dict.items():

                    if candidate_key_tuple not in key_rank.keys():
                        key_rank[candidate_key_tuple] = 0

                    with out:
                        print_line()
                        if contr_or_compl_df_list[0] == "contradictory":
                            print("Candidate key " + Colors.CBOLD + str(
                                list(candidate_key_tuple)) + Colors.CEND + " yields "
                                  + Colors.CREDBG + contr_or_compl_df_list[0] + Colors.CEND + " values")
                        else:
                            print("Candidate key " + Colors.CBOLD + str(
                                list(candidate_key_tuple)) + Colors.CEND + " yields "
                                  + Colors.CGREENBG + contr_or_compl_df_list[0] + Colors.CEND + " values")

                    if contr_or_compl_df_list[0] == "contradictory":
                        # print(contr_or_compl_df_list)
                        row1_dfs = []
                        row2_dfs = []

                        skip_this_pair = False
                        preferred_view_set = set()

                        for row_tuple in contr_or_compl_df_list[1:]:

                            # TODO: epsilon greedy
                            #  If the user selected one contradictory row (n times) more frequently over the other,
                            #  skip this contradiction

                            exclude_this_contradiction = False
                            if p > epsilon:
                                row1_strs = row_df_to_string(row_tuple[0])
                                row2_strs = row_df_to_string(row_tuple[1])
                                for row1 in row1_strs:
                                    for row2 in row2_strs:
                                        if (row_rank[row1] > n * row_rank[row2] and row_rank[row1] - row_rank[
                                            row2] > n) or \
                                                (row_rank[row2] > n * row_rank[row1] and row_rank[row2] - row_rank[
                                                    row1] > n):
                                            preferred_view = path1 if row_rank[row1] > n * row_rank[row2] else path2
                                            preferred_view_set.add(preferred_view)
                                            # exclude this particular contradiction
                                            exclude_this_contradiction = True

                                if exclude_this_contradiction:
                                    continue

                            row1_dfs.append(row_tuple[0])
                            row2_dfs.append(row_tuple[1])

                        # concatenate all contradictory rows in both side
                        if len(row1_dfs) > 0 and len(row2_dfs) > 0:

                            contradictory_rows1 = pd.concat(row1_dfs).reset_index(drop=True)
                            contradictory_rows2 = pd.concat(row2_dfs).reset_index(drop=True)

                            html1 = contradictory_rows1.style \
                                .applymap(highlight_cols, subset=pd.IndexSlice[:, list(candidate_key_tuple)],
                                          color='lightyellow') \
                                .apply(highlight_diff, axis=None, df2=contradictory_rows2) \
                                .render()

                            html2 = contradictory_rows2.style \
                                .applymap(highlight_cols, subset=pd.IndexSlice[:, list(candidate_key_tuple)],
                                          color='lightyellow') \
                                .apply(highlight_diff, axis=None, df2=contradictory_rows1) \
                                .render()

                            count += 1
                            print_option(count, html1, buttons)
                            option_dict[count] = (candidate_key_tuple, row1_dfs, path1)

                            count += 1
                            print_option(count, html2, buttons)
                            option_dict[count] = (candidate_key_tuple, row2_dfs, path2)
                        else:
                            if best_key != None:
                                # If there's already a preferred key
                                if len(preferred_view_set) == 1:
                                    # If the user always select all the contradictory rows in one view over the other
                                    # then skip this pair
                                    preferred_view = preferred_view_set.pop()
                                    with out:
                                        print("Skipping this pair...")
                                        print("Automatically selecting preferred view " + preferred_view)
                                    view_rank[preferred_view] += 1
                                    break
                            else:
                                # Otherwise skip showing this contradiction for the current key
                                with out:
                                    print("Automatically skipping all contradictions based on previous selections")

                    if contr_or_compl_df_list[0] == "complementary":
                        # TODO: epsilon greedy for complementary rows?
                        #  But they are not really "choose one over the other" relationship

                        # concatenate all complementary (non-intersecting) rows in both side
                        complementary_df_tuple = contr_or_compl_df_list[1]

                        if len(complementary_df_tuple[0]) > 0:
                            complementary_part1 = pd.concat(complementary_df_tuple[0])
                            html1 = complementary_part1.style.applymap(highlight_cols, subset=pd.IndexSlice[:, list(
                                candidate_key_tuple)]).render()

                            count += 1
                            print_option(count, html1, buttons)
                            option_dict[count] = (candidate_key_tuple, complementary_df_tuple[0], path1)
                        if len(complementary_df_tuple[1]) > 0:
                            complementary_part2 = pd.concat(complementary_df_tuple[1])
                            html2 = complementary_part2.style.applymap(highlight_cols, subset=pd.IndexSlice[:, list(
                                candidate_key_tuple)]).render()

                            count += 1
                            print_option(count, html2, buttons)
                            option_dict[count] = (candidate_key_tuple, complementary_df_tuple[1], path2)

            if len(option_dict) > 0:

                num_interactions += 1

                # option_list = [str(option) for option in option_dict.keys()]
                # option_list.append("Skip")
                # option_list.append("Stop")
                #
                # toggle_buttons = ToggleButtons(
                #     options=[str(option) for option in option_dict.keys()],
                #     description='Options:',
                #     disabled=False,
                #     button_style='', # 'success', 'info', 'warning', 'danger' or ''
                #     # tooltips=['Description of slow', 'Description of regular', 'Description of fast'],
                # #     icons=['check'] * 3
                # )
                #
                #
                option_picked = await wait_for_change(buttons)
                #
                # clear_output()
                # out.clear_output()
                # display(out)
                # display(toggle_buttons)
                #         clear_output()
                #         out.clear_output()

                # option_picked = input(
                #     Colors.CWHITEBG + "Select option (0: skip, empty: stop): " + Colors.CEND)

                if option_picked == "Stop":
                    with out:
                        print("Stopped interaction")
                    break

                # while not (option_picked.isdigit() and
                #            (int(option_picked) in option_dict.keys() or int(option_picked) == 0)):
                #     option_picked = input(
                #         Colors.CWHITEBG + "Select option (0: skip, empty: stop): " + Colors.CEND)
                #
                # option_picked = int(option_picked)

                if option_picked != "Skip":
                    option_picked = int(option_picked[7:])

                    candidate_key_picked = option_dict[option_picked][0]
                    if candidate_key_picked != None:
                        key_rank[candidate_key_picked] += 1

                    # TODO： Add score for any view containing the contradictory or complementary row selected
                    # views_to_add_score = set()
                    rows_picked = option_dict[option_picked][1]
                    for row_df in rows_picked:
                        row_strs = row_df_to_string(row_df)

                        for row_str in row_strs:
                            if row_str in row_to_path_dict.keys():
                                paths_containing_row = row_to_path_dict[row_str]
                                for path in paths_containing_row:
                                    # views_to_add_score.add(path)
                                    view_rank[path] += 1

                            if row_str in row_rank.keys():
                                row_rank[row_str] += 1

                    # for path in views_to_add_score:
                    #     view_rank[path] += 1

        sorted_view_rank = sort_view_by_scores(view_rank)
        return sorted_view_rank, num_interactions

    task = asyncio.ensure_future(f())
    display(out)

    return task


######################################UNCERTAINTY FRAMEWORK######################################


def create_signals(view_files, all_pair_contr_compl, sample_size):
    # contradictions[key][(row1, row2)] = (row1_df, row2_df, views1, views2)
    Contradiction = namedtuple("Contradiction", ["row1_df", "row2_df", "views1", "views2"])
    contradictions = defaultdict(lambda: defaultdict(Contradiction))

    # complements[key][(row1, row2)] = (row1_df, row2_df, views1, views2)
    Complement = namedtuple("Complement", ["row1_df", "row2_df", "views1", "views2"])
    complements = defaultdict(lambda: defaultdict(Complement))

    # key: distinct row in all views, value: set of views containing the row
    row_to_path_dict = {}

    # views that are either contradictory or complementary
    contr_or_compl_views = set()

    candidate_keys = set()

    for path, result in tqdm(all_pair_contr_compl.items()):
        path1 = path[0]
        path2 = path[1]

        contr_or_compl_views.add(path1)
        contr_or_compl_views.add(path2)

        if not (path1 in view_files and path2 in view_files):
            continue

        df1 = pd.read_csv(path1, encoding='latin1', thousands=',')
        df1 = mva.curate_view(df1)
        df1 = v4c.normalize(df1)

        df2 = pd.read_csv(path2, encoding='latin1', thousands=',')
        df2 = mva.curate_view(df2)
        df2 = v4c.normalize(df2)

        for candidate_key_tuple, result_list in result.items():

            candidate_keys.add(candidate_key_tuple)

            complementary_keys1 = result_list[0]
            complementary_keys2 = result_list[1]
            contradictory_keys = result_list[2]

            if len(contradictory_keys) > 0:

                # a list containing candidate key names
                candidate_key = list(candidate_key_tuple)
                # a list of tuples, each tuple corresponds to the contradictory key values
                key_values = list(contradictory_keys)

                if len(key_values) > sample_size:
                    key_values = random.sample(key_values, k=sample_size)

                # there could be multiple contradictions existing in a pair of views
                for key_value in key_values:
                    # select row based on multiple composite keys
                    row1_df = get_row_from_key(df1, candidate_key, key_value)
                    row2_df = get_row_from_key(df2, candidate_key, key_value)

                    # In some case one key can correspond to more than 1 row, we only take the first row as one row
                    # will be sufficient
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

            elif len(complementary_keys1) > 0 and len(complementary_keys2) > 0:

                candidate_key = list(candidate_key_tuple)
                key_values1 = list(complementary_keys1)
                key_values2 = list(complementary_keys2)

                key_combinations = [(key_value1, key_value2) for key_value1 in key_values1 for key_value2 in
                                    key_values2]
                if (len(key_combinations)) > sample_size:
                    key_combinations = random.sample(key_combinations, k=sample_size)

                for key_value1, key_value2 in key_combinations:
                    row1_df = get_row_from_key(df1, candidate_key, key_value1)
                    row2_df = get_row_from_key(df2, candidate_key, key_value2)

                    row1_str = row_df_to_string(row1_df)[0]
                    row2_str = row_df_to_string(row2_df)[0]

                    p1 = path1
                    p2 = path2

                    complement_already_exist = False
                    if candidate_key_tuple in complements.keys():
                        if (row1_str, row2_str) in complements[candidate_key_tuple]:
                            complement_already_exist = True
                        elif (row2_str, row1_str) in complements[candidate_key_tuple]:
                            row1_str, row2_str = row2_str, row1_str
                            row1_df, row2_df = row2_df, row1_df
                            p1, p2 = path2, path1
                            complement_already_exist = True

                    if complement_already_exist:
                        complement = complements[candidate_key_tuple][(row1_str, row2_str)]
                        complement.views1.add(p1)
                        complement.views2.add(p2)
                    else:
                        complement = Complement(row1_df=row1_df, row2_df=row2_df, views1={p1}, views2={p2})

                    complements[candidate_key_tuple][(row1_str, row2_str)] = complement

    non_contr_or_compl_view_files = view_files - contr_or_compl_views
    singletons = []
    for path in non_contr_or_compl_view_files:

        df = pd.read_csv(path, encoding='latin1', thousands=',')
        df = mva.curate_view(df)
        df = v4c.normalize(df)

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
    signals["complements"] = complements
    signals["singletons"] = singletons

    return signals, list(candidate_keys)


def get_sorted_views_in_top_percentile(view_scores, top_percentile):
    scores = np.array(list(view_scores.values()))
    threshold = np.percentile(scores, top_percentile)
    # print(threshold)

    epsilon = 10e-3
    top_views = [(view, score) for view, score in
                 sorted(view_scores.items(), key=lambda item: item[1], reverse=True)
                 if score >= threshold or abs(score - threshold) < epsilon]

    return top_views


def pick_best_signal_to_present(signals, best_key, view_scores, top_percentile):
    # pick the best signal, considering only the views with scores in the top_percentile
    views_to_consider = set()
    scores = np.array(list(view_scores.values()))
    threshold = np.percentile(scores, top_percentile)
    # print(threshold)
    epsilon = 10e-3
    for view, score in view_scores.items():
        if score >= threshold or abs(score - threshold) < epsilon:
            views_to_consider.add(view)
    # print("views_to_consider: ", views_to_consider)

    # If the user randomly picks a branch in the split, what's the expected value of uncertainty removed?
    # ev = sum over p_i * x_i
    # gain = total size - ev

    def compute_gain(split):
        expected_value = 0.0
        split_size = sum(split)
        if split_size == 0:
            return 0
        for num in split:
            expected_value += num / split_size * num
        gain = split_size - expected_value
        return gain

    max_gain = -1
    candidate_best_signal = []
    candidate_signal_to_delete = []
    for signal_type, s in signals.items():

        if signal_type == "contradictions" or signal_type == "complements":

            key = best_key
            if key not in s.keys():
                # only choose a random key if currently there are no best signals
                if len(candidate_best_signal) == 0 and len(list(s.keys())) > 0:
                    key = random.choice(list(s.keys()))
                else:
                    continue
            for row_tuple, vtuple in s[key].items():

                # print("views1: ", vtuple.views1)
                # print("views2: ", vtuple.views2)
                # print(views_to_consider)
                views1 = views_to_consider.intersection(set(vtuple.views1))
                views2 = views_to_consider.intersection(set(vtuple.views2))
                # print("new views1: ", views1)
                # print("new views2: ", views2)

                if len(views1) == 0 and len(views2) == 0:
                    continue

                split = [len(views1), len(views2)]

                gain = compute_gain(split)

                if abs(gain - max_gain) < epsilon:
                    best_signal = (signal_type, s[key][row_tuple], key)
                    signal_to_delete = (key, row_tuple)
                    candidate_best_signal.append(best_signal)
                    candidate_signal_to_delete.append(signal_to_delete)
                elif gain > max_gain:
                    best_signal = (signal_type, s[key][row_tuple], key)
                    signal_to_delete = (key, row_tuple)
                    candidate_best_signal = [best_signal]
                    candidate_signal_to_delete = [signal_to_delete]
                    max_gain = gain

        # TODO: force the singleton signal to always be the first to present
        elif signal_type == "singletons":
            views = [view for view, _ in s]
            # print(views)
            views = views_to_consider.intersection(set(views))
            # print(views)

            if len(views) == 0:
                continue

            # one view per branch
            split = [1] * len(views)

            gain = compute_gain(split)
            # gain = math.inf

            if abs(gain - max_gain) < epsilon:
                best_signal = (signal_type, s, None)
                candidate_best_signal.append(best_signal)
                candidate_signal_to_delete.append(None)
            elif gain > max_gain:
                best_signal = (signal_type, s, None)
                candidate_best_signal = [best_signal]
                candidate_signal_to_delete = [None]
                max_gain = gain

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

    return best_signal
