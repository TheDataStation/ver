import os
import pprint
import time
from collections import defaultdict

import pandas as pd
import numpy as np

import four_c

def clear_dir(path):
    for f in os.listdir(path):
        os.remove(os.path.join(path, f))

if __name__ == "__main__":

    num_views_list = [500, 1000, 2000]
    num_rows = 10000

    key_col = np.arange(num_rows)
    other_col = np.zeros(num_rows, dtype=int)

    base_view = pd.DataFrame({"key": key_col, "other": other_col})
    # print(base_view)

    # data_dir = "/Users/zhiruzhu/Desktop/Niffler/aurum-dod-staging/DoD/new_ver_4c/synthetic_views"
    data_dir = "/home/cc/aurum-dod-staging/DoD/new_ver_4c/synthetic_views"

    # log = open("/Users/zhiruzhu/Desktop/Niffler/aurum-dod-staging/DoD/new_ver_4c/log.txt", "w")
    log = open("/home/cc/aurum-dod-staging/DoD/new_ver_4c/log.txt", "w")
    times = defaultdict(list)

    for num_views in num_views_list:

        print(f"Num views: {num_views}")

        # Compatible
        # Generate n identical base views
        clear_dir(f"{data_dir}/compatible/")
        for i in range(num_views):
            base_view.to_csv(f"{data_dir}/compatible/{i}.csv", index=False)

        # Contained
        # For i in n:
        # Remove 1 random row in base view
        # Base view becomes the new view
        # Add the new view to the list
        clear_dir(f"{data_dir}/contained/")
        base_contained = base_view.copy()
        base_contained.to_csv(f"{data_dir}/contained/0.csv", index=False)
        for i in range(1, num_views):
            # num_rows_to_remove = np.random.randint(1, len(base_contained)+1)
            drop_indices = np.random.choice(base_contained.index)
            base_contained = base_contained.drop(drop_indices)
            base_contained.to_csv(f"{data_dir}/contained/{i}.csv", index=False)


        # Complementary
        # Max = n
        # For i in n:
        # Change 1 of random key value in base view to some other value (while still maintaining uniqueness and not causing contradiction)
        # random number = m
        # For j in range(m):
        # Change the key value to max + j
        # Max = n + m
        # Add the new view to the list
        clear_dir(f"{data_dir}/complementary/")
        max_key_value = num_views - 1
        for i in range(num_views):
            random_index = np.random.choice(base_view.index)
            new_view = base_view.copy()
            new_view.iloc[random_index]["key"] = max_key_value + 1
            max_key_value += 1
            new_view.to_csv(f"{data_dir}/complementary/{i}.csv", index=False)


        # Contradictory
        # For i in n:
        # Choose 1 random row in base view and change the non-key column value to some other value
        # Add the new view to the list
        clear_dir(f"{data_dir}/contradictory/")
        max_other_value = 0
        for i in range(num_views):
            random_index = np.random.choice(base_view.index)
            new_view = base_view.copy()
            new_view.iloc[random_index]["other"] = max_other_value + 1
            max_other_value += 1
            new_view.to_csv(f"{data_dir}/contradictory/{i}.csv", index=False)


        groups = ["compatible", "contained", "contradictory", "complementary"]

        for group in groups:
            input_path = f"/Users/zhiruzhu/Desktop/Niffler/aurum-dod-staging/DoD/new_ver_4c/synthetic_views/{group}/"

            start_time = time.time()
            four_c.main(input_path)
            elapsed = time.time() - start_time

            print(f"total 4c time for {group}: {elapsed} s")
            times[num_views].append(elapsed)
            log.write(str(elapsed) + " ")
        log.write("\n")

    log.close()
    pprint.pprint(times)

