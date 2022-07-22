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
    experiment_num = ""

    # data_dir = "/Users/zhiruzhu/Desktop/Niffler/aurum-dod-staging/DoD/new_ver_4c/data"
    # new_dir_path = f"/Users/zhiruzhu/Desktop/Niffler/aurum-dod-staging/DoD/new_ver_4c/test_dir{experiment_num}/"
    # results_dir = "results"
    # #
    # data_dir = "/home/cc/output_views_small"
    # provenance_dir = "/home/cc/output_views_join_paths"
    results_dir = f"/home/cc/zhiru/aurum-dod-staging/DoD/new_ver_4c/results{experiment_num}"

    random_seed = 0
    random.seed(a=random_seed)

    sample_portion_list = [0.25, 0.50, 0.75, 1.0]
    find_all_contradictions = True

    all_tables = []
    with open(f"/home/cc/all_tables.txt", "r") as f:
        lines = f.readlines()
        for line in lines:
            all_tables.append(line.strip())
    print(f"num tables: {len(all_tables)}")
    # print(all_tables)

    # result_dirs = [f"{data_dir}/jp710_full", f"{data_dir}/jp_1204", f"{data_dir}/jp_1572"]
    # result_dirs = [f"jp_1572"]

    # view_dirs.sort()
    # view_dirs = view_dirs[10*(partition_num-1):10*partition_num]
    # print(result_dirs)

    for k in range(2):
        if k == 0:
            data_dir = "/home/cc/output_views_small"
            provenance_dir = "/home/cc/output_views_join_paths"
        else:
            data_dir = "/home/cc/output_views_small2"
            provenance_dir = "/home/cc/output_views_join_paths2"

        view_dirs = [name for name in os.listdir(data_dir)
                     if os.path.isdir(os.path.join(data_dir, name))]

        for i, view_dir in enumerate(view_dirs):

            # print(f"view dir name: {view_dir}")
            if view_dir == "jp_313" or view_dir == "jp_356":
                continue

            view_files = glob.glob(f"{data_dir}/{view_dir}/view_*")

            provenance = {}
            # with open(f"{data_dir}/{view_dir}/log.txt", "r") as log:
            #     lines = log.readlines()
            #     chunk_size = 4
            #     if "jp710_full" in view_dir:
            #         chunk_size = 3
            #     chunks = [lines[x:x + chunk_size] for x in range(0, len(lines), chunk_size)]
            #     for chunk in chunks:
            #         line = chunk[1].strip()
            #         tables = set(re.split(pattern=", .+ JOIN |, .+", string=line))
            #         if '' in tables:
            #             tables.remove('')
            #         view_name = f"{data_dir}/{view_dir}/{chunk[0].strip()}"
            #         provenance[view_name] = tables

            df = pd.read_csv(f"{provenance_dir}/{view_dir}/join_paths.csv")
            provenance_tables = df[["tbl1", "tbl2"]].values.tolist()

            for view in view_files:
                result = re.search('view_(\d*).csv', view)
                view_num = int(result.group(1))
                tables = provenance_tables[view_num]
                if np.nan in tables:
                    tables.remove(np.nan)
                provenance[view] = set(tables)

            # pprint.pprint(provenance)

            base_sample_size = int(sample_portion_list[0] * len(all_tables))
            base_sample = random.sample(all_tables, base_sample_size)
            # num_views = []

            times = np.zeros((len(sample_portion_list), 7))
            results = np.zeros((len(sample_portion_list), 8))
            schema_groups = []

            for j, sample_portion in enumerate(sample_portion_list):

                print()
                print(f"view dir name: {view_dir}")
                print(f"sample portion = {sample_portion}")

                sample_size = int(sample_portion * len(all_tables))
                rest_of_tables = set(all_tables) - set(base_sample)
                sampled_tables = random.sample(list(rest_of_tables), sample_size - base_sample_size) + base_sample

                # print(len(sampled_tables))

                views = []
                for view, tables in provenance.items():
                    if tables.issubset(sampled_tables):
                        views.append(view)

                # clear_dir(new_dir_path)
                # for view in views:
                #     shutil.copy(view, new_dir_path)

                # num_views.append(len(views))

                start_time = time.time()

                compatible_groups, removed_compatible_views, \
                contained_groups, removed_contained_views, \
                total_candidate_complementary_contradictory_views, \
                complementary_groups, \
                contradictory_groups, total_num_contradictory_pairs, \
                find_compatible_contained_time_total, \
                get_df_time, classify_per_table_schema_time, \
                total_identify_c1_time, total_identify_c2_time, total_num_comparisons_c2, \
                find_complementary_contradictory_time_total, \
                schema_group, total_num_rows = \
                    v4c.main(input_path="", view_paths=views, find_all_contradictions=find_all_contradictions)

                elapsed = time.time() - start_time

                complementary_pairs = flatten(complementary_groups)
                contradictory_pairs = flatten(contradictory_groups)

                if sample_portion > 0.9:
                    # sample portion = 1.0
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
                print("-----------------------------------------------")

                # results.append([len(views), len(compatible_groups), len(contained_groups),
                #                 len(flatten(complementary_groups)), len(flatten(contradictory_groups))])
                results[j] = [len(views), total_num_rows,
                                 num_views_after_pruning_compatible, num_views_after_pruning_contained,
                                 num_complementary_pairs, total_num_contradictory_pairs,
                                 total_num_contradictions, total_num_comparisons_c2]
                times[j] = [elapsed, get_df_time, classify_per_table_schema_time,
                               total_identify_c1_time, total_identify_c2_time,
                               find_compatible_contained_time_total, find_complementary_contradictory_time_total]
                schema_groups.append(schema_group)

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
                for schema_group in schema_groups:
                    for num in schema_group:
                        f.write(str(num) + " ")
                    f.write("\n")

    # clear_dir(new_dir_path)
