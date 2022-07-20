import glob
import os
import pprint
import random
import shutil
import time
from collections import defaultdict
import re

import numpy as np

import four_c as v4c


def clear_dir(path):
    for f in os.listdir(path):
        os.remove(os.path.join(path, f))

def flatten(alist):
    return [x for l in alist for x in l]


if __name__ == '__main__':
    # data_dir = "/Users/zhiruzhu/Desktop/Niffler/aurum-dod-staging/DoD/new_ver_4c/data"
    # new_dir_path = "/Users/zhiruzhu/Desktop/Niffler/aurum-dod-staging/DoD/new_ver_4c/test_dir/"
    data_dir = "/home/cc/output_views2"
    new_dir_path = "/home/cc/zhiru/aurum-dod-staging/DoD/new_ver_4c/test_dir/"
    sample_portion_list = [0.25, 0.5, 0.75, 1.0]

    all_tables = []
    with open(f"{data_dir}/all_tables.txt", "r") as f:
        lines = f.readlines()
        for line in lines:
            all_tables.append(line.strip())
    print(f"num table: {len(all_tables)}")
    # print(all_tables)

    result_dirs = [f"{data_dir}/jp710_full", f"{data_dir}/jp_1204", f"{data_dir}/jp_1572"]
    # result_dirs = [os.path.join(data_dir, name) for name in os.listdir(data_dir)
    #                if os.path.isdir(os.path.join(data_dir, name))]
    # print(result_dirs)

    times = np.zeros((len(result_dirs), len(sample_portion_list)))
    results = np.zeros((len(result_dirs), len(sample_portion_list), 5))

    for i, dir in enumerate(result_dirs):

        print(f"dir name: {dir}")

        view_files = glob.glob(f"{dir}/view_*")

        provenance = {}
        with open(f"{dir}/log.txt", "r") as log:
            lines = log.readlines()
            chunk_size = 4
            if "jp710_full" in dir:
                chunk_size = 3
            chunks = [lines[x:x + chunk_size] for x in range(0, len(lines), chunk_size)]
            for chunk in chunks:
                line = chunk[1].strip()
                tables = set(re.split(pattern=", .+ JOIN |, .+", string=line))
                if '' in tables:
                    tables.remove('')
                view_name = f"{dir}/{chunk[0].strip()}"
                provenance[view_name] = tables

        # pprint.pprint(provenance)

        base_sample_size = int(sample_portion_list[0] * len(all_tables))
        base_sample = random.sample(all_tables, base_sample_size)
        # num_views = []

        for j, sample_portion in enumerate(sample_portion_list):

            print(f"sample portion = {sample_portion}")

            sample_size = int(sample_portion * len(all_tables))
            rest_of_tables = set(all_tables) - set(base_sample)
            sampled_tables = random.sample(list(rest_of_tables), sample_size - base_sample_size) + base_sample

            views = []
            for view, tables in provenance.items():
                if tables.issubset(sampled_tables):
                    views.append(view)

            clear_dir(new_dir_path)
            for view in views:
                shutil.copy(view, new_dir_path)

            # num_views.append(len(views))

            start_time = time.time()

            path_to_df_dict, \
            compatible_groups, removed_compatible_views, \
            contained_groups, removed_contained_views, \
            complementary_groups, \
            contradictory_groups, \
            all_pair_results = \
                v4c.main(new_dir_path, find_all_contradictions=True)

            elapsed = time.time() - start_time
            # print(i, j)
            times[i][j] = elapsed

            num_views_after_pruning_compatible = len(views) - len(removed_compatible_views)
            num_views_after_pruning_contained = num_views_after_pruning_compatible - len(removed_contained_views)

            print("-----------------------------------------------")
            # print(f"num compatible groups: {len(compatible_groups)}")
            # print(f"num contained views: {len(contained_groups)}")
            print(f"num_views_after_pruning_compatible: {num_views_after_pruning_compatible}")
            print(f"num_views_after_pruning_contained: {num_views_after_pruning_contained}")
            print(f"num complementary pairs: {len(flatten(complementary_groups))}")
            print(f"num contradictions: {len(flatten(contradictory_groups))}")
            print(f"time: {elapsed}")

            # results.append([len(views), len(compatible_groups), len(contained_groups),
            #                 len(flatten(complementary_groups)), len(flatten(contradictory_groups))])
            results[i][j] = [len(views), num_views_after_pruning_compatible, num_views_after_pruning_contained,
                             len(flatten(complementary_groups)), len(flatten(contradictory_groups))]

    print(times)
    np.save("times.npy", times)

    # with open("times.log", "w") as f:
    #     for time in times:
    #         f.write(str(time) + "\n")

    print(results)
    np.save("results.npy", results)
    # with open("results.log", "w") as f:
    #     for result in results:
    #         for num in result:
    #             f.write(str(num) + " ")
    #         f.write("\n")
    clear_dir(new_dir_path)

