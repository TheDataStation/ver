from presentation_utils import *
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
from collections import defaultdict, namedtuple
from tabulate import tabulate

pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)
pd.set_option('display.max_colwidth', None)
pd.set_option('display.width', None)


class Mode(Enum):
    manual = 1,
    random = 2,
    optimal = 3


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

                    contradiction_already_exist = False
                    if candidate_key_tuple in contradictions.keys():
                        if (row1_str, row2_str) in contradictions[candidate_key_tuple]:
                            contradiction_already_exist = True
                        elif (row2_str, row1_str) in contradictions[candidate_key_tuple]:
                            row1_str, row2_str = row2_str, row1_str
                            row1_df, row2_df = row2_df, row1_df
                            path1, path2 = path2, path1
                            contradiction_already_exist = True

                    if contradiction_already_exist:
                        contradiction = contradictions[candidate_key_tuple][(row1_str, row2_str)]
                        contradiction.views1.add(path1)
                        contradiction.views2.add(path2)
                    else:
                        contradiction = Contradiction(row1_df=row1_df, row2_df=row2_df, views1={path1}, views2={path2})

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

                    complement_already_exist = False
                    if candidate_key_tuple in complements.keys():
                        if (row1_str, row2_str) in complements[candidate_key_tuple]:
                            complement_already_exist = True
                        elif (row2_str, row1_str) in complements[candidate_key_tuple]:
                            row1_str, row2_str = row2_str, row1_str
                            row1_df, row2_df = row2_df, row1_df
                            path1, path2 = path2, path1
                            complement_already_exist = True

                    if complement_already_exist:
                        complement = complements[candidate_key_tuple][(row1_str, row2_str)]
                        complement.views1.add(path1)
                        complement.views2.add(path2)
                    else:
                        complement = Complement(row1_df=row1_df, row2_df=row2_df, views1={path1}, views2={path2})

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

    return signals, candidate_keys


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
            if best_key not in s.keys():
                best_key = random.choice(list(s.keys()))
            for row_tuple, vtuple in s[best_key].items():
                # print(vtuple.views1, vtuple.views2)
                # print(views_to_consider)
                views1 = views_to_consider.intersection(set(vtuple.views1))
                views2 = views_to_consider.intersection(set(vtuple.views2))

                if len(views1) == 0 or len(views2) == 0:
                    continue
                # print(views1, views2)
                split = [len(views1), len(views2)]

                gain = compute_gain(split)

                if abs(gain - max_gain) < epsilon:
                    best_signal = (signal_type, s[best_key][row_tuple], best_key)
                    signal_to_delete = (best_key, row_tuple)
                    candidate_best_signal.append(best_signal)
                    candidate_signal_to_delete.append(signal_to_delete)
                elif gain > max_gain:
                    best_signal = (signal_type, s[best_key][row_tuple], best_key)
                    signal_to_delete = (best_key, row_tuple)
                    candidate_best_signal = [best_signal]
                    candidate_signal_to_delete = [signal_to_delete]
                    max_gain = gain

        elif signal_type == "singletons":
            views = [view for view, _ in s]
            views = views_to_consider.intersection(set(views))

            if len(views) == 0:
                continue

            # one view per branch
            split = [1] * len(views)

            gain = compute_gain(split)

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
        random_idx = random.randint(0, len(candidate_best_signal)-1)
        best_signal = candidate_best_signal[random_idx]
        if best_signal[0] == "singletons":
            del signals["singletons"]
        else:
            signal_to_delete = candidate_signal_to_delete[random_idx]
            del signals[best_signal[0]][signal_to_delete[0]][signal_to_delete[1]]

    return best_signal


if __name__ == '__main__':

    #################################CONFIG#####################################
    dir_path = "./building/"
    # top percentile of view scores to include in window
    top_percentiles = [0, 25, 50, 75, 100]
    # max size of candidate (composite) key
    candidate_key_size = 2
    # sampling 5 contradictory or complementary rows from each view to include in the presentation
    sample_size = 5

    mode = Mode.optimal

    max_num_interactions = 100

    num_runs = 20

    ground_truth_path = "./building/view_0"
    fact_bank_fraction = 0.5

    result_dir = "./result_uncertainty/building/"
    ############################################################################

    # Run 4C
    print(Colors.CBOLD + "--------------------------------------------------------------------------" + Colors.CEND)
    print("Running 4C...")

    compatible_groups, contained_groups, complementary_groups, contradictory_groups, all_pair_contr_compl = \
        v4c.main(dir_path, candidate_key_size)

    print()
    print(Colors.CBOLD + "--------------------------------------------------------------------------" + Colors.CEND)

    view_files = glob.glob(dir_path + "/view_*")
    print("Number of views: ", len(view_files))

    view_files = prune_compatible_views(view_files, compatible_groups)
    print("After pruning compatible views: ", len(view_files))

    view_files = prune_contained_views(view_files, contained_groups)
    print("After pruning contained views: ", len(view_files))


    fact_bank_df = None
    if mode == Mode.optimal:
        print("Ground truth view: " + ground_truth_path)
        fact_bank_df = pd.read_csv(ground_truth_path, encoding='latin1', thousands=',')
        fact_bank_df = mva.curate_view(fact_bank_df)
        fact_bank_df = v4c.normalize(fact_bank_df)
        fact_bank_df = fact_bank_df.sample(frac=fact_bank_fraction)

    result_by_top_percentile = []
    cur_max_interactions = 0
    for top_percentile in top_percentiles:

        print("Top percentile = ", str(top_percentile))

        ground_truth_rank = []
        for run in range(num_runs):

            print("Run " + str(run))

            print(
                Colors.CBOLD + "--------------------------------------------------------------------------" +
                Colors.CEND)
            print("Creating signals...")

            signals, candidate_keys = create_signals(view_files, all_pair_contr_compl, sample_size)

            # Initialize ranking model
            key_rank = list(candidate_keys)
            random.shuffle(key_rank)

            # row_rank = row_to_path_dict.copy()
            # for row, path in row_rank.items():
            #     row_rank[row] = 0
            view_scores = {}
            for path in view_files:
                view_scores[path] = 0

            num_interactions = 0
            ground_truth_rank_per_run = []
            while num_interactions < max_num_interactions:
                print(
                    Colors.CBOLD + "--------------------------------------------------------------------------" +
                    Colors.CEND)

                # pick top key from key_rank
                best_key = key_rank[0]
                best_signal = pick_best_signal_to_present(signals, best_key, view_scores, top_percentile)

                if best_signal == None:
                    # we have explored all the signals
                    break
                signal_type, signal, best_key = best_signal

                num_interactions += 1

                # present options
                options = []
                options_to_print = []
                headers = []
                if signal_type == "contradictions" or signal_type == "complements":

                    print("Key = ", list(best_key))
                    row1_df, row2_df, views1, views2 = signal

                    options = [[1, row1_df, views1], [2, row2_df, views2]]
                    options_to_print = [[1, row1_df.to_string(index=False), views1],
                                        [2, row2_df.to_string(index=False), views2]]
                    headers = ["Option", "", "Views"]
                    if signal_type == "contradictions":
                        headers[1] = "Contradictory Row"
                    else:
                        headers[1] = "Complementary Row"

                elif signal_type == "singletons":
                    for i, t in enumerate(signal, 1):
                        view, sample_df = t
                        options.append([i, sample_df, [view]])
                        options_to_print.append([i, sample_df.to_string(index=False), [view]])

                    headers = ["Option", "Sample rows", "View"]

                print(tabulate(options_to_print, headers, tablefmt="grid"))

                # select option
                option_picked = 0
                valid_options = [0] + [option for option, df, views in options]

                if mode == Mode.optimal:

                    max_intersection_with_fact_back = 0
                    for option, df, views in options:
                        column_intersections = df.columns.intersection(fact_bank_df.columns)
                        # print(column_intersections)
                        if len(column_intersections) > 0:
                            # default to intersection
                            intersection = pd.merge(left=df[list(column_intersections)],
                                                    right=fact_bank_df[list(column_intersections)],
                                                    on=None)
                            # print(intersection)
                            intersection = intersection.drop_duplicates()
                            # print(intersection)
                            if intersection.size > max_intersection_with_fact_back:
                                # Always selection the option that's more consistent with the fact bank
                                # if there's no intersection, then skip this option (select 0)
                                option_picked = option
                                max_intersection_with_fact_back = intersection.size
                                # print(str(max_intersection_with_fact_back) + " " + str(option_picked))
                    print(Colors.CGREYBG + "Select option (or 0 if no preferred option): " + Colors.CEND)
                    print("Optimal option = " + str(option_picked))

                elif mode == Mode.random:
                    option_picked = random.choice(valid_options)
                    print(Colors.CGREYBG + "Select option (or 0 if no preferred option): " + Colors.CEND)
                    print("Random option = " + str(option_picked))

                else:
                    option_picked = input(
                        Colors.CGREYBG + "Select option (or 0 if no preferred option): " + Colors.CEND)

                    if option_picked == "":
                        break

                    while not (option_picked.isdigit() and int(option_picked) in valid_options):
                        option_picked = input(
                            Colors.CGREYBG + "Select option (or 0 if no preferred option): " + Colors.CEND)

                    option_picked = int(option_picked)

                # update rank
                if option_picked != 0:
                    option, df, views = options[option_picked-1]
                    for view in views:
                        view_scores[view] += 1
                else:
                    # didn't select any option, move down the current key's rank to the bottom (to make sure other keys
                    # get chances of being presented)
                    if signal_type == "contradictions" or signal_type == "complements":
                        key_rank.append(key_rank.pop(key_rank.index(best_key)))

                # pprint.pprint(sort_view_by_scores(view_scores))

                print(Colors.CREDBG2 + "Views in top " + str(top_percentile) + " percentile" + Colors.CEND)
                top_views = get_sorted_views_in_top_percentile(view_scores, top_percentile)
                pprint.pprint(top_views)

                if mode == Mode.optimal or mode == Mode.random:
                    rank = get_view_rank_with_ties(view_scores, ground_truth_path)
                    if rank != None:
                        # ground_truth_rank[run][loop_count] = rank
                        ground_truth_rank_per_run.append(rank)
                    else:
                        print("ERROR!!! Did not find " + ground_truth_path + " in view rank")
                        exit()

            print(Colors.CREDBG2 + "Final Views" + Colors.CEND)
            sorted_views = sort_view_by_scores(view_scores)
            pprint.pprint(sorted_views)

            if mode == Mode.optimal or mode == Mode.random:
                rank = get_view_rank_with_ties(view_scores, ground_truth_path)
                if rank != None:
                    # pprint.pprint(sort_view_by_scores(view_scores))
                    print("Ground truth view " + ground_truth_path + " is top-" + str(rank))
                    print("Number of interactions: ", num_interactions)
                    print(
                        Colors.CBOLD + "--------------------------------------------------------------------------" +
                        Colors.CEND)
                ground_truth_rank.append(ground_truth_rank_per_run)
                if len(ground_truth_rank_per_run) > cur_max_interactions:
                    cur_max_interactions = len(ground_truth_rank_per_run)

        # ground_truth_rank_np = np.array(ground_truth_rank)
        result_by_top_percentile.append(ground_truth_rank)

    result_by_top_percentile_new = []
    for ground_truth_rank in result_by_top_percentile:
        ground_truth_rank_new = []
        for ground_truth_rank_per_run in ground_truth_rank:
            ground_truth_rank_per_run_new = ground_truth_rank_per_run
            if len(ground_truth_rank_per_run) < cur_max_interactions:
                ground_truth_rank_per_run_new += [np.nan] * (cur_max_interactions - len(ground_truth_rank_per_run))
            ground_truth_rank_new.append(ground_truth_rank_per_run_new)
        result_by_top_percentile_new.append(ground_truth_rank_new)

    result_by_top_percentile_np = np.array(result_by_top_percentile_new)
    np.save(result_dir + "result_by_top_percentile", result_by_top_percentile_np)
