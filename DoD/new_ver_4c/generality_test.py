import glob
import os
import pprint
import random
import shutil
import time
from collections import defaultdict
import re

import numpy as np
import pandas as pd

import four_c as v4c


def clear_dir(path):
    for f in os.listdir(path):
        os.remove(os.path.join(path, f))


def flatten(alist):
    return [x for l in alist for x in l]


if __name__ == '__main__':

    # partition_num = 5
    # experiment_num = "_keyword"

    # data_dir = "/Users/zhiruzhu/Desktop/Niffler/aurum-dod-staging/DoD/new_ver_4c/data"
    # new_dir_path = f"/Users/zhiruzhu/Desktop/Niffler/aurum-dod-staging/DoD/new_ver_4c/test_dir{experiment_num}/"
    # results_dir = "results"
    # #
    data_dir = "/home/cc/generality_experiment/views_keyword"
    # provenance_dir = "/home/cc/output_views_join_paths"
    results_dir = f"/home/cc/zhiru/aurum-dod-staging/DoD/new_ver_4c/keyword_4c_results"

    random_seed = 0
    random.seed(a=random_seed)

    # sample_portion_list = [0.25, 0.50, 0.75, 1.0]
    find_all_contradictions = True
    dropna = False
    candidate_key_size=1

    # all_tables = []
    # with open(f"/home/cc/all_tables.txt", "r") as f:
    #     lines = f.readlines()
    #     for line in lines:
    #         all_tables.append(line.strip())
    # print(f"num tables: {len(all_tables)}")
    # print(all_tables)

    # result_dirs = [f"{data_dir}/jp710_full", f"{data_dir}/jp_1204", f"{data_dir}/jp_1572"]
    # result_dirs = [f"jp_1572"]

    # view_dirs.sort()
    # view_dirs = view_dirs[10*(partition_num-1):10*partition_num]
    # print(result_dirs)

    view_dirs = [name for name in os.listdir(data_dir)
                 if os.path.isdir(os.path.join(data_dir, name))]
    view_dirs.sort()

    for i, view_dir in enumerate(view_dirs):

        print(f"query num: {i}")

        print(f"view dir name: {view_dir}")

        views = glob.glob(f"{data_dir}/{view_dir}/views/view_*")

        start_time = time.time()

        compatible_groups, removed_compatible_views, \
        contained_groups, removed_contained_views, \
        total_candidate_complementary_contradictory_views, \
        complementary_groups, \
        contradictory_groups, total_num_contradictory_pairs, \
        find_compatible_contained_time_total, \
        get_df_time, classify_per_table_schema_time, \
        total_identify_c1_time, total_identify_c2_time, total_num_comparisons_c2, \
        find_complementary_contradictory_time_total, total_find_candidate_keys_time, \
        schema_group, total_num_rows = \
            v4c.main(input_path="", view_paths=views,
                     find_all_contradictions=find_all_contradictions,
                     dropna=dropna,
                     candidate_key_size=candidate_key_size)

        elapsed = time.time() - start_time

        complementary_pairs = flatten(complementary_groups)
        contradictory_pairs = flatten(contradictory_groups)

        with open(f"{results_dir}/sainyam/pruned_compatible_views_{view_dir}.txt", "w") as f:
            for view in removed_compatible_views:
                f.write(view + "\n")

        with open(f"{results_dir}/sainyam/pruned_contained_views_{view_dir}.txt", "w") as f:
            for view in removed_contained_views:
                f.write(view + "\n")

        with open(f"{results_dir}/sainyam/candidate_compl_views_{view_dir}.txt", "w") as f:
            for view in total_candidate_complementary_contradictory_views:
                f.write(view + "\n")

        with open(f"{results_dir}/sainyam/compl_contra_pairs_{view_dir}.txt", "w") as f:
            for path1, path2, candidate_key in complementary_pairs:
                f.write(f"{path1}, {path2}, {candidate_key}\n")
            for path1, path2, candidate_key, contradictory_key_value in contradictory_pairs:
                f.write(f"{path1}, {path2}, {candidate_key}, {contradictory_key_value}\n")

        # print(i, j)

        num_views_after_pruning_compatible = len(views) - len(removed_compatible_views)
        num_views_after_pruning_contained = num_views_after_pruning_compatible - len(removed_contained_views)

        print("-----------------------------------------------")
        # print(f"num compatible groups: {len(compatible_groups)}")
        # print(f"num contained views: {len(contained_groups)}")
        print(f"original num views: {len(views)}")
        print(f"total_num_rows: {total_num_rows}")
        print(f"num_views_after_pruning_compatible: {num_views_after_pruning_compatible}")
        print(f"num_views_after_pruning_contained: {num_views_after_pruning_contained}")
        print(f"total_num_comparisons_c2: {total_num_comparisons_c2}")
        num_complementary_pairs = len(complementary_pairs)
        print(f"num complementary pairs: {num_complementary_pairs}")
        print(f"num contradictory pairs: {total_num_contradictory_pairs}")
        total_num_contradictions = len(contradictory_pairs)
        print(f"total num contradictions: {total_num_contradictions}")
        print()

        print(f"total time: {elapsed}")
        print(f"get_df_time: {get_df_time}")
        print(f"classify_per_table_schema_time: {classify_per_table_schema_time}")
        print(f"total_identify_c1_time: {total_identify_c1_time}")
        print(f"total_identify_c2_time: {total_identify_c2_time}")
        print(f"find_compatible_contained_time_total: {find_compatible_contained_time_total}")
        print(f"find_complementary_contradictory_time_total: {find_complementary_contradictory_time_total}")
        print(f"total_find_candidate_keys_time: {total_find_candidate_keys_time}")
        print("-----------------------------------------------")

        results = np.array([len(views), total_num_rows,
                      num_views_after_pruning_compatible, num_views_after_pruning_contained,
                      num_complementary_pairs, total_num_contradictory_pairs,
                      total_num_contradictions, total_num_comparisons_c2])
        times = np.array([elapsed, get_df_time, classify_per_table_schema_time,
                    total_identify_c1_time, total_identify_c2_time,
                    find_compatible_contained_time_total, find_complementary_contradictory_time_total,
                    total_find_candidate_keys_time])
        # schema_groups.append(schema_group)

        # print(times)
        np.save(f"{results_dir}/times_{view_dir}.npy", times)

        # with open("times.log", "w") as f:
        #     for time in times:
        #         f.write(str(time) + "\n")

        # print(results)
        np.save(f"{results_dir}/results_{view_dir}.npy", results)
        # with open("results.log", "w") as f:
        #     for result in results:
        #         for num in result:
        #             f.write(str(num) + " ")
        #         f.write("\n")

        with open(f"{results_dir}/schema_groups_{view_dir}.txt", "w") as f:
            for num in schema_group:
                f.write(str(num) + " ")
            f.write("\n")

        # clear_dir(new_dir_path)
