import time

from pandas.core.util.hashing import hash_pandas_object

from utils import *

class ViewDistillation:

    def __init__(self, path_to_views):
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

        self.complementary_pairs = set()

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

            # if path1 in compatible_views_to_remove \
            #         or path1 in already_classified_as_contained:
            #     i += 1
            #     continue

            # largest_contained_view = None
            j = i + 1

            while j < len(dfs):
                (df2, path2) = dfs[j]

                # if path2 in compatible_views_to_remove \
                #         or path2 in already_classified_as_contained:
                #     j += 1
                #     continue

                hash_set1 = self.hash_dict[path1]
                hash_set2 = self.hash_dict[path2]

                # num_comparisons += 1
                if hash_set2.issubset(hash_set1):
                    # view2 is contained in view1
                    # if largest_contained_view is None:
                    #     largest_contained_view = (df1, path1)
                    # else:
                    #     if len(df1) > len(largest_contained_view[0]):
                    #         already_classified_as_contained.add(largest_contained_view[1])
                    #         largest_contained_view = (df1, path1)
                    # already_classified_as_contained.add(path2)

                    already_contained = False
                    for k, v in contained_groups.items():
                        if path1 in v:
                            # view k contains path1, so it also contains path2, since path1 contains path2
                            contained_groups[k].add(path2)
                            already_contained = True
                    if not already_contained:
                        contained_groups[path1].add(path2)

                elif hash_set1.issubset(hash_set2):
                    # view1 is contained in view2
                    # if largest_contained_view is None:
                    #     largest_contained_view = (df2, path2)
                    # else:
                    #     if len(df2) > len(largest_contained_view[0]):
                    #         already_classified_as_contained.add(largest_contained_view[1])
                    #         largest_contained_view = (df2, path2)
                    # already_classified_as_contained.add(path1)

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

            # if largest_contained_view is not None:
            #     largest_contained_views.add(largest_contained_view[1])

        # to_be_removed = compatible_views_to_remove.union(already_classified_as_contained)

        # candidate_complementary_contradictory_view_paths = []
        # for df, path in dfs:
        #     if path not in to_be_removed:
        #         candidate_complementary_contradictory_views.append((df, path))
        #         candidate_complementary_contradictory_view_paths.append(path)

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

        return contained_lists

    def prune_contained_views(self, keep_largest=True):

        if not self.found_contained_views:
            self.find_contained_views()

        views_left = []

        for key, dfs in self.dfs_per_schema.items():
            new_dfs = []
            for df, path in dfs:
                if path not in self.contained_views_to_remove:
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

        return contradictions

    def _find_contradictory_views(self, dfs):

        candidate_key_to_inverted_index, view_to_candidate_keys_dict, find_candidate_keys_time = \
            build_inverted_index(
                dfs,
                self.candidate_key_size,
                self.uniqueness_threshold)

        start_time = time.time()

        # all_contradictory_pair_result = defaultdict(lambda: defaultdict(set))
        contractions = defaultdict(set)

        # map[key][key_value][(rowA, rowB)]=[views with rowA, views with rowB]
        # contradictions = defaultdict(lambda: defaultdict(lambda: defaultdict(list)))

        already_classified_as_contradictory = set()
        # complementary_pairs = set()

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
                    # for path1 in cluster:
                    lst = list(cluster)
                    i = 0
                    while i < len(lst):
                        path1 = lst[i]
                        j = i + 1
                        while j < len(lst):
                            # for path2 in cluster:
                            path2 = lst[j]
                            if (path1, path2, candidate_key) not in self.complementary_pairs and \
                                    (path2, path1, candidate_key) not in self.complementary_pairs:
                                self.complementary_pairs.add((path1, path2, candidate_key))
                            j += 1
                        i += 1

                already_added = set()
                for row1, cluster1 in clusters.items():

                    for row2, cluster2 in clusters.items():

                        # print(row1)
                        # print(row2)

                        if row1 == row2:
                            continue

                        for path1 in cluster1:
                            for path2 in cluster2:

                                # if not find_all_contradictions:
                                #     if (path1, path2, candidate_key) in already_classified_as_contradictory:
                                #         continue
                                if path1 == path2:
                                    continue
                                if (path1, path2) in already_added:
                                    continue

                                # all_contradictory_pair_result[(path1, path2)][candidate_key].add(key_value)
                                contractions[(path1, path2, candidate_key)].add(key_value)

                                already_added.add((path1, path2))
                                already_added.add((path2, path1))

                                already_classified_as_contradictory.add((path1, path2, candidate_key))
                                already_classified_as_contradictory.add((path2, path1, candidate_key))

        self.complementary_pairs = self.complementary_pairs - already_classified_as_contradictory


        # complementary_pairs = []
        # contradictory_pairs = []
        # for path, v1 in all_contradictory_pair_result.items():
        #     path1, path2 = path
        #     for candidate_key, contradictory_key_values in v1.items():
        #         if len(contradictory_key_values) > 0:
        #             for contradictory_key_value in contradictory_key_values:
        #                 contradictory_pairs.append((path1, path2, candidate_key, contradictory_key_value))

        # print(f"total_find_complementary_time: {time.time() - start_time} s")

        # num_contradictory_pairs = len(all_contradictory_pair_result)

        return contractions

    def find_complementary_views(self):

        if not self.found_contradictory_views:
            self.find_contradictory_views()

        return self.complementary_pairs


if __name__ == "__main__":
    vd = ViewDistillation("/Users/zhiruzhu/Desktop/Niffler/ver/view_distillation/toytest/")
    res = vd.find_contained_views()
    print(res)
    res = vd.prune_contained_views()
    print(res)
    # print(vd.dfs_per_schema)
    res = vd.find_contradictory_views()
    print(res)