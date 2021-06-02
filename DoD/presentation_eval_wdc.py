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

    # root_dir = "/home/cc/experiments_chembl_5_13/"
    root_dir = "./experiments_wdc_5_13/"
    eval_file = open(root_dir + "eval.txt", "w")
    for query in range(5):
        query_dir = root_dir + "wdc_gt" + str(query) + "/"
        for noise in ["zero_noise", "mid_noise", "high_noise"]:
            noise_dir = query_dir + noise + "/"
            sample_dir = noise_dir + "sample0/"
            for pipeline in range(1, 4):

                if pipeline == 1 or pipeline == 2:
                    continue

                #################################CONFIG#####################################

                dir_path = sample_dir + "result" + str(pipeline) + "/"
                print("\n\n")
                print("Running in dir: ", dir_path)

                # top percentile of view scores to include in window
                top_percentile = 25
                # top_percentiles = [25]
                # max size of candidate (composite) key
                candidate_key_size = 2
                # sampling 5 contradictory or complementary rows from each view to include in the presentation
                sample_sizes = [1, 5, 10]

                mode = Mode.optimal

                max_num_interactions = 100

                num_runs = 50

                initialize_scores = ["zero", "s4"]
                fact_bank_fractions = [10, 50, 100]
                # fact_bank_fraction = 1

                # result_dir = dir_path.replace(root_dir, "/home/cc/zhiru/presentation_results_chembl_5_16/")
                result_dir = dir_path.replace(root_dir, "./test_dir/")
                Path(result_dir).mkdir(parents=True, exist_ok=True)

                # initialize_score = "s4"
                # if pipeline == 1 or pipeline == 2:
                #     initialize_score = "s4"
                # print("Initialize score with ", initialize_score)

                ################################LOG FILE###################################

                log_path = dir_path + "log.txt"
                log_file = open(log_path, "r")
                lines = log_file.readlines()

                dod_score = {}
                s4_score = {}
                cur_view = None
                # cur_s4_score = None
                # cur_dod_score = None
                ground_truth_view = None
                time_before_view_pre = None
                for line in lines:
                    if line.startswith("view"):
                        cur_view = dir_path + line[:-1].replace("view", "view_") + ".csv"
                    if line.startswith("s4_score"):
                        if pipeline == 3:
                            lst = line.split(sep=", ")
                            cur_s4_score = float(lst[0].split(sep=": ")[1])
                            s4_score[cur_view] = cur_s4_score
                            cur_dod_score = float(lst[1].split(sep=": ")[1])
                            dod_score[cur_view] = cur_dod_score
                        else:
                            cur_s4_score = float(line.split(sep=": ")[1])
                            s4_score[cur_view] = cur_s4_score
                    if line.startswith("ground truth view"):
                        ground_truth_view = line.split(sep=": ")[1][:-1]
                        ground_truth_view = ground_truth_view.replace("view", "view_")

                    if line.startswith("total_time"):
                        time_before_view_pre = float(line.split(sep=": ")[1])
                log_file.close()

                ground_truth_path = dir_path + ground_truth_view

                #####################################4C#####################################

                start_time_4c = time.time()

                # Run 4C
                print(
                    Colors.CBOLD + "--------------------------------------------------------------------------" + Colors.CEND)
                print("Running 4C...")

                original_view_files = glob.glob(dir_path + "/view_*")
                if len(original_view_files) > len(s4_score):
                    for i in range(len(s4_score), len(original_view_files)):
                        view_to_remove = Path(dir_path + "view_" + str(i) + ".csv")
                        view_to_remove.unlink()
                original_view_files = glob.glob(dir_path + "/view_*")

                compatible_groups, contained_groups, complementary_groups, contradictory_groups, all_pair_contr_compl = \
                    v4c.main(dir_path, candidate_key_size)

                print()
                print(
                    Colors.CBOLD + "--------------------------------------------------------------------------" + Colors.CEND)

                print("Number of views: ", len(original_view_files))

                eval_file.write(str(len(original_view_files)) + ",")
                rank = get_view_rank_with_ties(s4_score, ground_truth_path)
                print("s4 rank before pruning = " + str(rank))
                eval_file.write(str(rank) + ",")

                view_files = prune_compatible_views(original_view_files, compatible_groups)
                print("After pruning compatible views: ", len(view_files))
                num_views_after_prune_compatible = len(view_files)

                view_files = prune_contained_views(view_files, contained_groups)
                print("After pruning contained views: ", len(view_files))

                # time_4c = time.time() - start_time_4c
                # file_4c = open(result_dir + "log.txt", "w")
                # file_4c.write("Total time before view presentation:" + str(time_before_view_pre) + "\n")
                # file_4c.write("4c time(s):" + str(time_4c) + "\n")
                # file_4c.write("original num of views:" + str(len(original_view_files)) + "\n")
                # file_4c.write("After pruning compatible views:" + str(num_views_after_prune_compatible) + "\n")
                # file_4c.write("After pruning contained views:" + str(len(view_files)) + "\n")
                # file_4c.close()

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
                                df = pd.read_csv(view, encoding='latin1', thousands=',')
                                df = mva.curate_view(df)

                                if len(df) > max_size:
                                    max_size = len(df)
                                    largest_view = view
                            ground_truth_path = largest_view
                            break
                assert (ground_truth_path in view_files)

                eval_file.write(str(len(view_files)) + ",")
                view_scores = {}
                for path in view_files:
                    view_scores[path] = s4_score[path]
                rank = get_view_rank_with_ties(view_scores, ground_truth_path)
                print("s4 rank after pruning = " + str(rank))
                eval_file.write(str(rank) + ",")

                print(
                    Colors.CBOLD + "--------------------------------------------------------------------------" +
                    Colors.CEND)
                print("Creating signals...")

                signals, candidate_keys = create_signals_multi_row(view_files, all_pair_contr_compl,
                                                                   sample_size=5)

                def compute_informativeness(split, total_num_views):
                    expected_value = 0.0
                    split_size = sum(split)
                    if split_size == 0:
                        return 0
                    for num in split:
                        expected_value += num / split_size * num
                    informativeness = expected_value / total_num_views
                    return informativeness

                max_informativeness = -1
                epsilon = 1e-8
                for signal_type, s in signals.items():

                    if signal_type == "contradictions" or signal_type == "complements":

                        for key in s.keys():
                            for row_tuple, vtuple in s[key].items():

                                split = [len(vtuple.views1), len(vtuple.views2)]

                                informativeness = compute_informativeness(split, len(view_files))

                                if informativeness > max_informativeness:
                                    max_informativeness = informativeness

                    elif signal_type == "singletons":
                        views = [view for view, _ in s]

                        if len(views) == 0:
                            continue

                        # one view per branch
                        split = [1] * len(views)

                        informativeness = compute_informativeness(split, len(view_files))

                        if informativeness > max_informativeness:
                            max_informativeness = informativeness

                print("max_informativeness = ", str(max_informativeness))

                #TODO: For a signal to be informative, the expected number of views we can update should be
                # more than 10% of the total number of views
                threshold = 0.1
                if max_informativeness > 0.1:
                    print(str(query) + "," + noise + ": informative")
                    eval_file.write("informative\n")
                else:
                    print(str(query) + "," + noise + ": not informative")
                    eval_file.write("not informative\n")

    eval_file.close()