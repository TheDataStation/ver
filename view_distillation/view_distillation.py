import os.path
import time

import pandas as pd
from pandas.core.util.hashing import hash_pandas_object

from utils import *


class ViewDistillation:

    def __init__(self, path_to_views):

        self.path_to_views = path_to_views

        self.candidate_key_size = 2
        self.uniqueness_threshold = 0.9

        dfs, self.path_to_df_dict = get_dataframes(path_to_views)
        self.dfs_per_schema = classify_per_table_schema(dfs)

        self.hash_dict = {}

        self.found_compatible_views = False
        self.compatible_views_to_remove = set()

        self.found_contained_views = False
        self.contained_views_to_remove = set()

        self.found_contradictory_views = False

        self.contradictions = {}

        self.wrong_keys = set()
        self.pruned_contradictory_views = set()

    def get_current_views(self):

        current_views = []
        for key, dfs in self.dfs_per_schema.items():
            current_views += [path for df, path in dfs]
        return current_views

    def distill_views(self, remove_identical_views=True,
                      remove_contained_views=True,
                      union_complementary_views=True):

        if remove_identical_views:
            self.reduce_compatible_views_to_one()

        if remove_contained_views:
            self.prune_contained_views(keep_largest=True)

        if union_complementary_views:
            self.union_complementary_views()

        return self.get_current_views()

    def find_compatible_views(self):

        self.found_compatible_views = True

        compatible_groups = []

        for key, dfs in self.dfs_per_schema.items():
            cur_compatible_groups = self._find_compatible_views(dfs)
            compatible_groups += cur_compatible_groups

        return compatible_groups

    def _find_compatible_views(self, dfs):

        compatible_groups = []

        if len(dfs) == 1:
            # only one view, so no need to find compatible or contained
            df, path = dfs[0]
            compatible_groups.append([path])
        else:
            compatible_clusters = defaultdict(set)

            for df, path in dfs:
                df_hash = hash_pandas_object(df, index=False)

                hash_sum = df_hash.sum()

                self.hash_dict[path] = set(df_hash)

                compatible_clusters[hash_sum].add(path)

            # compatible_count = len(compatible_clusters.keys())

            for k, cluster in compatible_clusters.items():
                compatible_groups.append(list(cluster.copy()))
                # only keep the first compatible view
                cluster.pop()
                self.compatible_views_to_remove.update(cluster)

        return compatible_groups

    def reduce_compatible_views_to_one(self):

        if not self.found_compatible_views:
            self.find_compatible_views()

        views_left = []

        for key, dfs in self.dfs_per_schema.items():
            new_dfs = []
            for df, path in dfs:
                if path not in self.compatible_views_to_remove:
                    new_dfs.append((df, path))
                    views_left.append(path)
            self.dfs_per_schema[key] = new_dfs

        return views_left

    def find_contained_views(self):

        self.found_contained_views = True

        contained_groups = []

        for key, dfs in self.dfs_per_schema.items():
            cur_contained_groups = self._find_contained_views(dfs)
            contained_groups += cur_contained_groups

        return contained_groups

    def _find_contained_views(self, dfs):

        if len(self.hash_dict) == 0:
            for df, path in dfs:
                df_hash = hash_pandas_object(df, index=False)
                self.hash_dict[path] = set(df_hash)

        contained_groups = defaultdict(set)

        # num_comparisons = 0
        i = 0
        while i < len(dfs):

            if len(dfs) <= 1:
                break

            (df1, path1) = dfs[i]

            j = i + 1

            while j < len(dfs):
                (df2, path2) = dfs[j]

                hash_set1 = self.hash_dict[path1]
                hash_set2 = self.hash_dict[path2]

                if hash_set2.issubset(hash_set1):

                    already_contained = False
                    for k, v in contained_groups.items():
                        if path1 in v:
                            # view k contains path1, so it also contains path2, since path1 contains path2
                            contained_groups[k].add(path2)
                            already_contained = True
                    if not already_contained:
                        contained_groups[path1].add(path2)

                elif hash_set1.issubset(hash_set2):

                    already_contained = False
                    for k, v in contained_groups.items():
                        if path2 in v:
                            # view k contains path2, so it also contains path1, since path2 contains path1
                            contained_groups[k].add(path1)
                            already_contained = True
                    if not already_contained:
                        contained_groups[path2].add(path1)

                j += 1

            i += 1

        contained_lists = []
        for path1, paths in contained_groups.items():
            paths.add(path1)
            lst = list(paths)
            len_dict = {}
            for path in lst:
                len_dict[path] = len(self.path_to_df_dict[path])
            sorted_lst = sorted(lst, key=lambda path: len_dict[path], reverse=True)

            contained_lists.append(sorted_lst)

            self.contained_views_to_remove.update(set(sorted_lst[1:]))

        idx_to_remove = set()
        for i in range(len(contained_lists)):
            lst1 = contained_lists[i]
            for j in range(len(contained_lists)):
                if i != j:
                    lst2 = contained_lists[j]
                    if set(lst1).issubset(lst2):
                        idx_to_remove.add(i)
                    elif set(lst2).issubset(lst1):
                        idx_to_remove.add(j)
        for idx in sorted(idx_to_remove, reverse=True):
            del contained_lists[idx]

        return contained_lists

    def prune_contained_views(self, keep_largest=True):

        if not self.found_contained_views:
            self.find_contained_views()

        views_left = []

        for key, dfs in self.dfs_per_schema.items():
            new_dfs = []
            for df, path in dfs:
                if keep_largest:
                    if path not in self.contained_views_to_remove:
                        new_dfs.append((df, path))
                        views_left.append(path)
                else:
                    new_dfs.append((df, path))
                    views_left.append(path)
            self.dfs_per_schema[key] = new_dfs

        return views_left

    def find_contradictory_views(self):

        self.found_contradictory_views = True

        contradictions = {}
        for key, dfs in self.dfs_per_schema.items():
            cur_contradictions = self._find_contradictory_views(dfs)
            contradictions.update(cur_contradictions)

        self.contradictions = contradictions

        return contradictions

    def _find_contradictory_views(self, dfs):

        candidate_key_to_inverted_index, view_to_candidate_keys_dict, find_candidate_keys_time = \
            build_inverted_index(
                dfs,
                self.candidate_key_size,
                self.uniqueness_threshold)

        start_time = time.time()

        contractions = defaultdict(set)

        already_classified_as_contradictory = set()
        complementary_pairs = set()

        for candidate_key, inverted_index in tqdm(candidate_key_to_inverted_index.items()):

            for key_value, dfs in tqdm(inverted_index.items()):
                if len(dfs) <= 1:
                    # only one view for this key value, no need to compare
                    continue

                clusters = defaultdict(set)

                for df, path, idx in dfs:
                    row = tuple(df.iloc[idx])
                    clusters[row].add(path)

                for row, cluster in clusters.items():
                    lst = list(cluster)
                    i = 0
                    while i < len(lst):
                        path1 = lst[i]
                        j = i + 1
                        while j < len(lst):
                            path2 = lst[j]
                            if (path1, path2, candidate_key) not in complementary_pairs and \
                                    (path2, path1, candidate_key) not in complementary_pairs:
                                complementary_pairs.add((path1, path2, candidate_key))
                            j += 1
                        i += 1

                already_added = set()
                for row1, cluster1 in clusters.items():

                    for row2, cluster2 in clusters.items():

                        if row1 == row2:
                            continue

                        for path1 in cluster1:
                            for path2 in cluster2:

                                if path1 == path2:
                                    continue
                                if (path1, path2) in already_added:
                                    continue

                                contractions[(path1, path2, candidate_key)].add(key_value)

                                already_added.add((path1, path2))
                                already_added.add((path2, path1))

                                already_classified_as_contradictory.add((path1, path2, candidate_key))
                                already_classified_as_contradictory.add((path2, path1, candidate_key))

        self.complementary_pairs = list(complementary_pairs - already_classified_as_contradictory)

        return contractions

    def find_complementary_views(self):

        if not self.found_contradictory_views:
            self.find_contradictory_views()

        idx_to_remove = []
        for i in range(len(self.complementary_pairs)):
            path1, path2, candidate_key = self.complementary_pairs[i]
            if path1 in self.pruned_contradictory_views or path2 in self.pruned_contradictory_views or candidate_key \
                    in self.wrong_keys:
                idx_to_remove.append(i)

        for idx in sorted(idx_to_remove, reverse=True):
            del self.complementary_pairs[idx]

        return self.complementary_pairs

    def union_complementary_views(self):

        self.find_complementary_views()

        already_processed = set()
        views_to_remove = set()
        new_views = []

        for path1, path2, candidate_key in self.complementary_pairs:

            if (path1, path2) in already_processed:
                continue

            df1 = self.path_to_df_dict[path1]
            df2 = self.path_to_df_dict[path2]

            new_df = pd.concat([df1, df2]).drop_duplicates().reset_index(drop=True)
            file_name = f"{os.path.splitext(path1)[0]}_union_{path2}"
            new_path = os.path.join(self.path_to_views, file_name)
            new_df.to_csv(new_path)

            already_processed.add((path1, path2))
            views_to_remove.add(path1)
            views_to_remove.add(path2)
            new_views.append((new_path, new_df))

        views_left = []
        for key, dfs in self.dfs_per_schema.items():
            new_dfs = []
            for df, path in dfs:
                if path not in views_to_remove:
                    new_dfs.append((df, path))
                    views_left.append(path)

            self.dfs_per_schema[key] = new_dfs

        for path, df in new_views:
            the_hashes = [hash(el) for el in df.columns]
            schema_id = sum(the_hashes)
            self.dfs_per_schema[schema_id].append((df, path))

            views_left.append(path)

        return views_left

    def present_contradictory_views_demo(self):

        from ipywidgets import Output, Button, ToggleButtons, interact, fixed, HBox
        from IPython.display import display, clear_output, HTML

        import asyncio

        if not self.found_contradictory_views:
            self.find_contradictory_views()

        l = list(self.contradictions.items())
        random.shuffle(l)
        self.contradictions = dict(l)

        def wait_for_change(buttons):
            future = asyncio.Future()

            def getvalue(change):
                future.set_result(change.description)
                for button in buttons:
                    button.on_click(getvalue, remove=True)
                # we need to free up the binding to getvalue to avoid an InvalidState error
                # buttons don't support unobserve
                # so use `remove=True`

            for button in buttons:
                button.on_click(getvalue)
            return future

        out = Output()
        skip_button = Button(description="Skip")
        stop_button = Button(description="Stop")
        wrong_key_button = Button(description="Wrong key")

        @out.capture()
        def print_option(desc, html, buttons):
            button = Button(description=desc)
            display(button)
            buttons.append(button)
            display(HTML(html))

        async def f():
            num_interactions = 0
            views_pruned = set()
            views_selected = set()
            wrong_keys = set()

            for k, key_values in self.contradictions.items():

                path1, path2, key_tuple = k

                out.clear_output()

                skip = False
                if path1 in views_pruned or path2 in views_pruned or key_tuple in wrong_keys:
                    num_interactions += 1
                    skip = True

                if num_interactions >= len(self.contradictions):
                    # we have explored all the contradictory / complementary view pairs and single views at least once
                    with out:
                        print("You have explored all contradictory views")
                    break

                if skip:
                    continue

                num_interactions += 1

                buttons = [skip_button, stop_button, wrong_key_button]

                with out:
                    display(HBox([skip_button, stop_button, wrong_key_button]))

                with out:
                    print()
                    print(path1 + " - " + path2)
                    if len(key_tuple) == 1:
                        print("Key:", key_tuple[0])
                    else:
                        print("Key:", str(key_tuple))

                row1_dfs = []
                row2_dfs = []

                df1 = self.path_to_df_dict[path1]
                df2 = self.path_to_df_dict[path2]

                for key_value in key_values:
                    row1_df = get_row_from_key(df1, key_tuple, key_value)
                    row2_df = get_row_from_key(df2, key_tuple, key_value)

                    row1_dfs.append(row1_df)
                    row2_dfs.append(row2_df)

                # concatenate all contradictory rows in both side
                if len(row1_dfs) > 0 and len(row2_dfs) > 0:

                    contradictory_rows1 = pd.concat(row1_dfs).reset_index(drop=True)
                    contradictory_rows2 = pd.concat(row2_dfs).reset_index(drop=True)

                    html1 = contradictory_rows1.style \
                        .applymap(highlight_cols, subset=pd.IndexSlice[:, list(key_tuple)],
                                  color='lightyellow') \
                        .apply(highlight_diff, axis=None, df2=contradictory_rows2) \
                        .to_html()

                    html2 = contradictory_rows2.style \
                        .applymap(highlight_cols, subset=pd.IndexSlice[:, list(key_tuple)],
                                  color='lightyellow') \
                        .apply(highlight_diff, axis=None, df2=contradictory_rows1) \
                        .to_html()

                    print_option(path1, html1, buttons)

                    print_option(path2, html2, buttons)

                    option_picked = await wait_for_change(buttons)

                    if option_picked == "Stop":
                        with out:
                            print("Stopped interaction")
                        break

                    if option_picked == "Wrong key":
                        with out:
                            print("Wrong key")
                        wrong_keys.add(key_tuple)

                    elif option_picked != "Skip":

                        views_selected.add(option_picked)
                        views_pruned.add(option_picked)

                        with out:
                            print(f"You picked {option_picked}")

            out.clear_output()
            with out:
                print("End")

            views_left = []

            for key, dfs in self.dfs_per_schema.items():
                new_dfs = []
                for df, path in dfs:
                    if path not in views_pruned:
                        new_dfs.append((df, path))
                        views_left.append(path)
                self.dfs_per_schema[key] = new_dfs

            self.wrong_keys = wrong_keys
            self.pruned_contradictory_views = views_pruned

            return views_pruned, views_selected, views_left

        task = asyncio.ensure_future(f())
        display(out)

        return task

    def get_row_from_key(self, df: pd.DataFrame, key: tuple, key_value: tuple):

        return get_row_from_key(df, key, key_value)


if __name__ == "__main__":
    vd = ViewDistillation("/Users/zhiruzhu/Desktop/Niffler/ver/view_distillation/dataset/toytest/")

    res = vd.distill_views()
    print(res)
