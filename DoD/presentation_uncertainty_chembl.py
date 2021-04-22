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
from pathlib import Path
import time

if __name__ == '__main__':

    for query in range(6):
        query_dir = "/home/cc/experiments_chembl/chembl_gt" + str(query) + "/"
        for noise in ["zero_noise", "mid_noise", "high_noise"]:
            noise_dir = query_dir + noise + "/"
            sample_dir = noise_dir + "sample0/"
            s4_scores_path = sample_dir.replace("/home/cc/experiments_chembl/", "/home/cc/s4_score_chembl/")
            for pipeline in range(1, 4):

                #################################CONFIG#####################################

                dir_path = sample_dir + "result" + str(pipeline) + "/"
                print("\n\n")
                print("Running in dir: ", dir_path)

                # top percentile of view scores to include in window
                top_percentiles = [0, 25, 50, 75, 100]
                # max size of candidate (composite) key
                candidate_key_size = 2
                # sampling 5 contradictory or complementary rows from each view to include in the presentation
                sample_size = 5

                mode = Mode.optimal

                max_num_interactions = 100

                num_runs = 1

                result_dir = dir_path.replace("/home/cc/experiments_chembl/", "./presentation_results_chembl/")
                Path(result_dir).mkdir(parents=True, exist_ok=True)

                ################################GROUND TRUTH###################################

                log_path = dir_path + "log.txt"
                log_file = open(log_path, "w")
                lines = log_file.readlines()
                ground_truth_view = lines[-5].split(sep=": ")[1]
                log_file.close()

                ground_truth_path = dir_path + ground_truth_view
                fact_bank_fraction = 0.5

                ##################################S4 SCORE##########################################
                s4_score_path = s4_scores_path + "s4_score_pipeline" + str(pipeline) + ".txt"
                s4_score_file = open(s4_score_path, "r")
                s4_score = {}
                lines = s4_score_file.readlines()[:-1]
                for line in lines:
                    line_list = line.split()
                    view_path = dir_path + line_list[0]
                    s4_score[view_path] = float(line_list[1])
                s4_score_file.close()

                #####################################4C#####################################

                start_time_4c = time.time()

                # Run 4C
                print(Colors.CBOLD + "--------------------------------------------------------------------------" + Colors.CEND)
                print("Running 4C...")

                compatible_groups, contained_groups, complementary_groups, contradictory_groups, all_pair_contr_compl = \
                    v4c.main(dir_path, candidate_key_size)

                print()
                print(Colors.CBOLD + "--------------------------------------------------------------------------" + Colors.CEND)

                original_view_files = glob.glob(dir_path + "/view_*")
                print("Number of views: ", len(original_view_files))

                view_files = prune_compatible_views(original_view_files, compatible_groups)
                print("After pruning compatible views: ", len(view_files))

                view_files = prune_contained_views(view_files, contained_groups)
                print("After pruning contained views: ", len(view_files))

                time_4c = time.time() - start_time_4c
                file_4c = open(result_dir + "time_4c.txt", "w")
                file_4c.write("4c time(s):" + str(time_4c))
                file_4c.close()

                ############################################################################

                if ground_truth_path not in view_files:
                    for compatible_group in compatible_groups:
                        if ground_truth_path in compatible_group:
                            ground_truth_path = compatible_group[0]
                            break
                    for contained_group in contained_groups:
                        if ground_truth_path in contained_group:
                            max_size = 0
                            largest_view = contained_group[0]
                            for view in contained_group:
                                if len(view) > max_size:
                                    max_size = len(view)
                                    largest_view = view
                            ground_truth_path = largest_view
                            break

                fact_bank_df = None
                if mode == Mode.optimal:
                    print("Ground truth view: " + ground_truth_path)
                    fact_bank_df = pd.read_csv(ground_truth_path, encoding='latin1', thousands=',')
                    fact_bank_df = mva.curate_view(fact_bank_df)
                    fact_bank_df = v4c.normalize(fact_bank_df)
                    fact_bank_df = fact_bank_df.sample(frac=fact_bank_fraction)

                for initialize_score in ["zero", "s4"]:

                    result_by_top_percentile = []
                    time_by_top_percentile = []

                    cur_max_interactions = 0
                    for top_percentile in top_percentiles:

                        print("Top percentile = ", str(top_percentile))

                        times = np.empty(num_runs)
                        ground_truth_rank = []
                        for run in range(num_runs):

                            start_time_run = time.time()
                            print("Run " + str(run))

                            print(
                                Colors.CBOLD + "--------------------------------------------------------------------------" +
                                Colors.CEND)
                            print("Creating signals...")

                            signals, candidate_keys = create_signals(view_files, all_pair_contr_compl, sample_size)

                            # Initialize ranking model
                            key_rank = list(candidate_keys)
                            random.shuffle(key_rank)

                            view_scores = {}
                            if initialize_score == "zero":
                                for path in view_files:
                                    view_scores[path] = 0
                            elif initialize_score == "s4":
                                view_scores = s4_score

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

                                    # print("Key = ", list(best_key))
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

                                # print(tabulate(options_to_print, headers, tablefmt="grid"))

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

                                # print(Colors.CREDBG2 + "Views in top " + str(top_percentile) + " percentile" + Colors.CEND)
                                # top_views = get_sorted_views_in_top_percentile(view_scores, top_percentile)
                                # pprint.pprint(top_views)

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

                                times[run] = time.time() - start_time_run

                        # ground_truth_rank_np = np.array(ground_truth_rank)
                        result_by_top_percentile.append(ground_truth_rank)
                        time_by_top_percentile.append(times)

                    result_by_top_percentile_new = []
                    for ground_truth_rank in result_by_top_percentile:
                        ground_truth_rank_new = []
                        for ground_truth_rank_per_run in ground_truth_rank:
                            ground_truth_rank_per_run_new = ground_truth_rank_per_run
                            if len(ground_truth_rank_per_run) < cur_max_interactions:
                                ground_truth_rank_per_run_new += [np.nan] * (cur_max_interactions - len(ground_truth_rank_per_run))
                            ground_truth_rank_new.append(ground_truth_rank_per_run_new)
                        result_by_top_percentile_new.append(ground_truth_rank_new)

                    result_np_file_name = "result_by_top_percentile"
                    time_np_file_name = "time_by_top_percentile"
                    if initialize_score == "zero":
                        result_np_file_name += "_zero"
                        time_np_file_name += "_zero"
                    elif initialize_score == "s4":
                        result_np_file_name += "_s4"
                        time_np_file_name += "_s4"

                    result_by_top_percentile_np = np.array(result_by_top_percentile_new)
                    np.save(result_dir + result_np_file_name, result_by_top_percentile_np)

                    time_by_top_percentile_np = np.array(time_by_top_percentile)
                    np.save(result_dir + "time_by_top_percentile_zero", time_by_top_percentile_np)

