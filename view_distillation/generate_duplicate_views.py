import glob
import os
import pprint
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
    # dir_path = "../experiments_chembl_5_13/chembl_gt3/high_noise/sample0/result3/"
    dir_path = "toytest/"
    # new_dir_path = "test_dir/"

    original_view_files = glob.glob(dir_path + "view_*")
    num_original_views = len(original_view_files)

    # clear_dir(new_dir_path)

    # for view in original_view_files:
    #     shutil.copy(view, new_dir_path)

    results = []
    times = []

    for duplicate in [1]:

        # clear_dir(new_dir_path)
        #
        # for i in range(duplicate):
        #     for j, view in enumerate(original_view_files):
        #         df = pd.read_csv(view)
        #
        #         df = df.convert_dtypes()
        #
        #         for col in df:
        #             if pd.api.types.is_string_dtype(df[col]):
        #                 df[col] += str(i)
        #             elif pd.api.types.is_numeric_dtype(df[col]):
        #                 df[col] += i
        #             else:
        #                 print("error")
        #                 exit()
        #
        #         # df = df.replace(r'^\s*$', i, regex=True)
        #         # df = df.replace('', np.nan).fillna(str(i))
        #
        #         # df.columns = [name + str(i) for name in df.columns]
        #         view_num = num_original_views * (i) + j
        #         # print(view_num)
        #         df.to_csv(f"{new_dir_path}view_{view_num}.csv", index=False)
        #
        # time.sleep(1)

        # start_time = time.time()
        # _, compatible_groups, contained_groups, complementary_groups, contradictory_groups, _ = \
        #     v4c.main(dir_path, find_all_contradictions=True)
        # elapsed = time.time() - start_time
        #
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
        schema_group, total_num_rows \
            = v4c.main(dir_path, find_all_contradictions=True)
        elapsed = time.time() - start_time

        complementary_pairs = flatten(complementary_groups)
        contradictory_pairs = flatten(contradictory_groups)

        num_views_after_pruning_compatible = len(original_view_files) - len(removed_compatible_views)
        num_views_after_pruning_contained = num_views_after_pruning_compatible - len(removed_contained_views)

        print("-----------------------------------------------")
        # print(f"num compatible groups: {len(compatible_groups)}")
        # print(f"num contained views: {len(contained_groups)}")
        print(f"original num views: {len(original_view_files)}")
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

        # print("-----------------------------------------------")
        # print(f"num compatible groups: {len(compatible_groups)}")
        # print(f"num contained views: {len(contained_groups)}")
        # print(f"num complementary pairs: {len(flatten(complementary_groups))}")
        # print(f"num contradictions: {len(flatten(contradictory_groups))}")
        # print(f"time: {elapsed}")

        results.append([compatible_groups, contained_groups, removed_contained_views,
                        flatten(complementary_groups), flatten(contradictory_groups)])
        times.append(elapsed)

    print("-----------------------------------------------")

    # contradictory_views1 = results[0][4]
    # contradictory_views2 = results[1][4]
    # # print(contradictory_views1[:2])
    # # print(contradictory_views2[:2])
    # for contradiction1 in contradictory_views1:
    #     # path11, path21, candidate_key1, contradictory_key_value1 = contradiction1
    #     exist = False
    #     for contradiction2 in contradictory_views1:
    #         # path12, path22, candidate_key2, contradictory_key_value2 = contradiction2
    #         if contradiction1 == contradiction2:
    #             exist = True
    #     if not exist:
    #         print(contradiction1)
    #         exit()
    #
    # print("contradictory:")
    # print(contradictory_views1)
    # print(contradictory_views2)
    # print(len(set(contradictory_views2) - set(contradictory_views1)))


    # print(numbers)
    # complementary_views1 = set(results[0][3])
    # complementary_views2 = set(results[1][3])
    # print("complementary:")
    # pprint.pprint(complementary_views1)
    # pprint.pprint(complementary_views2)
    # pprint.pprint(complementary_views2 - complementary_views1)
    #
    # print("compatible:")
    # compatible1 = results[0][0]
    # compatible2 = results[1][0]
    # print(compatible1)
    # print(compatible2)


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
    # count = 0
    # single2 = set()
    # for compatible_group in results[1][0]:
    #     if len(compatible_group) == 1:
    #         # print(compatible_group)
    #         count += 1
    #         single2.add(compatible_group[0])
    #     else:
    #         print(compatible_group)
    # print(count)
    # #
    # # print(single1)
    # print(single2)
    #
    # print("contained")
    # contained1 = [results[0][1], results[0][2]]
    # contained2 = [results[1][1], results[1][2]]
    # print(contained1)
    # print(contained2)


    # complementary_views1_dup = complementary_views1

    # for t1 in complementary_views1:
    #     path1, path2, key = t1
    #     complementary_views1_dup.add()

    print(times)
