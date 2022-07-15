import glob
import os
import shutil
import time

import numpy as np
import pandas as pd

import four_c as v4c

def clear_dir(path):
    for f in os.listdir(path):
        os.remove(os.path.join(path, f))

def flatten(alist):
    return [x for l in alist for x in l]

if __name__ == '__main__':
    dir_path = "../experiments_chembl_5_13/chembl_gt3/high_noise/sample0/result3/"
    new_dir_path = "test_dir/"

    original_view_files = glob.glob(dir_path + "view_*")
    num_original_views = len(original_view_files)

    clear_dir(new_dir_path)

    # for view in original_view_files:
    #     shutil.copy(view, new_dir_path)

    results = []
    times = []

    for duplicate in [1, 2]:

        clear_dir(new_dir_path)

        for i in range(duplicate):
            for j, view in enumerate(original_view_files):
                df = pd.read_csv(view)

                df = df.convert_dtypes()

                for col in df:
                    if pd.api.types.is_string_dtype(df[col]):
                        df[col] += str(i)
                    elif pd.api.types.is_numeric_dtype(df[col]):
                        df[col] += i
                    else:
                        print("error")
                        exit()

                # df = df.replace(r'^\s*$', i, regex=True)
                # df = df.replace('', np.nan).fillna(str(i))

                # df.columns = [name + str(i) for name in df.columns]
                view_num = num_original_views * (i) + j
                # print(view_num)
                df.to_csv(f"{new_dir_path}view_{view_num}.csv", index=False)

        time.sleep(1)

        # start_time = time.time()
        # _, compatible_groups, contained_groups, complementary_groups, contradictory_groups, _ = \
        #     v4c.main(dir_path, find_all_contradictions=True)
        # elapsed = time.time() - start_time
        #
        start_time = time.time()
        _, compatible_groups_new, contained_groups_new, removed_contained_groups, complementary_groups_new, contradictory_groups_new, _ = \
            v4c.main(new_dir_path, find_all_contradictions=True)
        elapsed_new = time.time() - start_time

        # print("-----------------------------------------------")
        # print(f"num compatible views: {len(compatible_groups)}")
        # # print(compatible_groups)
        # print(f"num contained views: {len(contained_groups)}")
        # print(f"num complementary views: {len(flatten(complementary_groups))}")
        # print(f"num contradictory views: {len(flatten(contradictory_groups))}")
        # print(f"time: {elapsed}")

        print("-----------------------------------------------")
        print(f"num compatible groups: {len(compatible_groups_new)}")
        print(f"num contained views: {len(contained_groups_new)}")
        print(f"num complementary views: {len(flatten(complementary_groups_new))}")
        print(f"num contradictions: {len(flatten(contradictory_groups_new))}")
        print(f"time: {elapsed_new}")

        results.append([compatible_groups_new, contained_groups_new, removed_contained_groups,
                        flatten(complementary_groups_new), flatten(contradictory_groups_new)])
        times.append(elapsed_new)


    # print(numbers)
    complementary_views1 = set(results[0][3])
    complementary_views2 = set(results[1][3])
    print("complementary:")
    print(complementary_views1)
    print(complementary_views2)

    print("compatible:")
    compatible1 = results[1][0]
    compatible2 = results[0][0]
    print(compatible1)
    print(compatible2)


    # compatible1 = set([tuple(x) for x in results[1][0]])
    # compatible0 = set([tuple(x) for x in results[0][0]])

    # print(len(set(compatible1) - set(compatible0)))
    # print(compatible0)
    # print(set(compatible1) - set(compatible0))
    # count = 0
    # single1 = set()
    # for compatible_group in results[0][0]:
    #     if len(compatible_group) == 1:
    #         # print(compatible_group)
    #         count += 1
    #         single1.add(compatible_group[0])
    #     else:
    #         print(compatible_group)
    # print(count)
    count = 0
    single2 = set()
    for compatible_group in results[1][0]:
        if len(compatible_group) == 1:
            # print(compatible_group)
            count += 1
            single2.add(compatible_group[0])
        else:
            print(compatible_group)
    print(count)
    #
    # print(single1)
    print(single2)

    print("contained")
    contained1 = [results[0][1], results[0][2]]
    contained2 = [results[1][1], results[1][2]]
    print(contained1)
    print(contained2)


    # complementary_views1_dup = complementary_views1

    # for t1 in complementary_views1:
    #     path1, path2, key = t1
    #     complementary_views1_dup.add()

    print(times)
