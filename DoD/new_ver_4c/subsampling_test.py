import glob
import os
import pprint
import random
import shutil
import time
from collections import defaultdict
import re
import four_c as v4c


def clear_dir(path):
    for f in os.listdir(path):
        os.remove(os.path.join(path, f))

def flatten(alist):
    return [x for l in alist for x in l]


if __name__ == '__main__':
    dir_path = "jp710_full"
    new_dir_path = "test_dir/"

    view_files = glob.glob(dir_path + "view_*")

    provenance = {}
    with open(f"{dir_path}/log.txt", "r") as log:
        lines = log.readlines()
        chunks = [lines[x:x + 3] for x in range(0, len(lines), 3)]
        for i, chunk in enumerate(chunks):
            line = chunk[1].strip()
            tables = set(re.split(pattern=", .+ JOIN |, .+", string=line))
            if '' in tables:
                tables.remove('')
            view_name = f"{dir_path}/view_{i}.csv"
            provenance[view_name] = tables

    # pprint.pprint(provenance)
    all_tables = []
    with open("all_tables.txt", "r") as f:
        lines = f.readlines()
        for line in lines:
            all_tables.append(line.strip())
    print(f"num table: {len(all_tables)}")
    # print(all_tables)

    times = []
    results = []

    sample_portion_list = [0.25, 0.5, 0.75, 1.0]

    base_sample_size = int(sample_portion_list[0] * len(all_tables))
    base_sample = random.sample(all_tables, base_sample_size)
    num_views = []

    for sample_portion in sample_portion_list:

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

        num_views.append(len(views))

        start_time = time.time()

        path_to_df_dict, \
        compatible_groups, removed_compatible_groups, \
        contained_groups, removed_contained_groups, \
        complementary_groups, \
        contradictory_groups, \
        all_pair_results = \
            v4c.main(new_dir_path, find_all_contradictions=True)

        elapsed_new = time.time() - start_time
        times.append(elapsed_new)

        print("-----------------------------------------------")
        print(f"num compatible groups: {len(compatible_groups)}")
        print(f"num contained views: {len(contained_groups)}")
        print(f"num complementary pairs: {len(flatten(complementary_groups))}")
        print(f"num contradictions: {len(flatten(contradictory_groups))}")
        print(f"time: {elapsed_new}")

        results.append([len(views), len(compatible_groups), len(contained_groups),
                        len(flatten(complementary_groups)), len(flatten(contradictory_groups))])

    print(times)
    with open("times.log", "w") as f:
        for time in times:
            f.write(str(time) + "\n")

    print(results)
    with open("results.log", "w") as f:
        for result in results:
            for num in result:
                f.write(str(num) + " ")
            f.write("\n")
