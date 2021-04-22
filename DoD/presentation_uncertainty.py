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
from tabulate import tabulate

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
                    option, df, views = options[option_picked - 1]
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
