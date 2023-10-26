import os.path
import time

import networkx
import pandas as pd
from pandas.core.util.hashing import hash_pandas_object
import networkx as nx

from view_distillation.utils import *
import itertools
from pprint import pprint


class ViewDistillation:

    def __init__(self, path_to_views=None, dfs=None):

        self.path_to_views = path_to_views
        self.dfs = dfs

        self.candidate_key_size = 2
        self.uniqueness_threshold = 0.9

        dfs, self.path_to_df_dict = get_dataframes(path=path_to_views, dfs=dfs)
        self.dfs_per_schema = classify_per_table_schema(dfs)

        print(f"original num views: {len(dfs)}")
        # print(f"num schema groups: {len(self.dfs_per_schema)}")

        self.hash_dict = {}

        self.found_compatible_views = False
        self.compatible_views_to_remove = set()

        self.found_contained_views = False
        self.contained_views_to_remove = set()

        self.found_contradictory_views = False
        self.contradictions = {}

        self.complementary_pairs = []
        self.complementary_views_to_remove = set()
        self.unioned_complementary_views = []

        self.wrong_keys = set()
        self.pruned_contradictory_views = set()

        self.G = None

    def generate_graph(self):

        self.G = nx.DiGraph()

        compatible_groups = self.find_compatible_views()
        for group in compatible_groups:
            self.G.add_nodes_from(group)
            if len(group) > 1:
                edges = itertools.permutations(group, r=2)
                self.G.add_edges_from(edges, c="compatible")

        contained_groups = self.find_contained_views()
        for view1, lst in contained_groups.items():
            for view2 in lst:
                self.G.add_edge(view2, view1, c="contained")

        contradictions = self.find_contradictory_views()
        for view_pair, d in contradictions.items():
            view1, view2 = view_pair
            self.G.add_edge(view1, view2, c="contradictory", contradictory_key_values=d)
            self.G.add_edge(view2, view1, c="contradictory", contradictory_key_values=d)

        complementary_pairs = self.find_complementary_views()
        for view_pair, keys in complementary_pairs.items():
            view1, view2 = view_pair

            if self.G.has_edge(view1, view2) or self.G.has_edge(view2, view1):
                v1, v2 = view1, view2
                if self.G.has_edge(view2, view1):
                    v2, v1 = view1, view2
                if self.G[v1][v2]["c"] == "contained" or self.G[v1][v2]["c"] == "compatible":
                    # if the views are either compatible or contained, skip
                    # alternatively, can also label them as both compatible/contained and complementary
                    continue
                elif self.G[v1][v2]["c"] == "contradictory":
                    # contradictory for one key, complementary for other key
                    self.G[v1][v2]["c"] = "contradictory/complementary"
                    self.G[v2][v1]["c"] = "contradictory/complementary"
                    self.G[v1][v2]["complementary_keys"] = keys
                    self.G[v2][v1]["complementary_keys"] = keys
                    continue

            self.G.add_edge(view1, view2, c="complementary", complementary_keys=keys)
            self.G.add_edge(view2, view1, c="complementary", complementary_keys=keys)

        # print("nodes")
        # pprint(list(self.G.nodes.data()))
        # print("edges")
        # pprint(list(self.G.edges.data()))

        # self.draw_graph("graph")

        return self.G

    def prune_graph(self, remove_identical_views=True,
                    remove_contained_views=True,
                    union_complementary_views=True):

        if self.G is None:
            self.generate_graph()

        if remove_identical_views:
            self.reduce_compatible_views_to_one()
            self.G.remove_nodes_from(self.compatible_views_to_remove)

            print(f"num views after pruning compatible: {len(self.get_current_views())}")

        if remove_contained_views:
            self.prune_contained_views(keep_largest=True)
            self.G.remove_nodes_from(self.contained_views_to_remove)

            print(f"num views after pruning contained: {len(self.get_current_views())}")

        if union_complementary_views:
            self.union_complementary_views()
            self.G.remove_nodes_from(self.complementary_views_to_remove)
            self.G.add_nodes_from([path for path, df in self.unioned_complementary_views])

            print(f"num views after union complementary: {len(self.get_current_views())}")

        print(f"num of contradictory view paris: {len(self.contradictions)}")

        # print("nodes")
        # pprint(list(self.G.nodes.data()))
        # print("edges")
        # pprint(list(self.G.edges.data()))

        # self.draw_graph("pruned_graph")

        return self.G

    def draw_graph(self, filename):

        import matplotlib.pyplot as plt
        pos = nx.spring_layout(self.G)
        nx.draw(self.G, with_labels=True, pos=pos)
        edge_labels = nx.get_edge_attributes(self.G, 'c')
        formatted_edge_labels = {(elem[0], elem[1]): edge_labels[elem] for elem in
                                 edge_labels}  # use this to modify the tuple keyed dict if it has > 2 elements,
        nx.draw_networkx_edge_labels(self.G, pos, edge_labels=formatted_edge_labels)
        plt.savefig(filename, dpi=300)
        plt.close()

    def get_attributes(self, view1, view2):

        if not self.G.has_node(os.path.basename(view1)):
            print(f"node {view1} does not exist")
            return None
        if not self.G.has_node(os.path.basename(view2)):
            print(f"node {view2} does not exist")
            return None

        if not self.G.has_edge(view1, view2):
            if not self.G.has_edge(view2, view1):
                return None
            else:
                assert self.G[view2][view1]["c"] == "contained"
                print(f"{view2} is contained in {view1}")
                return None
        else:
            return self.G[view1][view2]

    def get_current_views(self):

        current_views = []
        for key, dfs in self.dfs_per_schema.items():
            current_views += [path for df, path in dfs]
        return current_views

    def get_df(self, view):

        file_name = os.path.basename(view)

        if file_name not in self.path_to_df_dict.keys():
            print(f"view does not exist")
            return None

        return self.path_to_df_dict[file_name]

    def get_dfs(self, views):

        dfs = []

        for view in views:
            file_name = os.path.basename(view)

            if file_name not in self.path_to_df_dict.keys():
                print(f"view {file_name} does not exist")
                continue

            dfs.append(self.path_to_df_dict[file_name])

        return dfs

    def distill_views(self, remove_identical_views=True,
                      remove_contained_views=True,
                      union_complementary_views=True):

        if remove_identical_views:
            self.reduce_compatible_views_to_one()

            print(f"num views after pruning compatible: {len(self.get_current_views())}")

        if remove_contained_views:
            self.prune_contained_views(keep_largest=True)

            print(f"num views after pruning contained: {len(self.get_current_views())}")

        if union_complementary_views:
            self.union_complementary_views()

            print(f"num views after union complementary: {len(self.get_current_views())}")

        print(f"num of contradictory view paris: {len(self.contradictions)}")

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

        if self.found_contradictory_views:
            self._remove_from_contra_compl(self.compatible_views_to_remove)

        return views_left

    def find_contained_views(self):

        self.found_contained_views = True

        contained_groups = defaultdict(set)

        for key, dfs in self.dfs_per_schema.items():
            cur_contained_groups = self._find_contained_views(dfs)
            contained_groups.update(cur_contained_groups)

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

            if path1 in self.contained_views_to_remove:
                i += 1
                continue

            j = i + 1

            while j < len(dfs):
                (df2, path2) = dfs[j]

                j += 1

                if path2 in self.contained_views_to_remove:
                    continue

                hash_set1 = self.hash_dict[path1]
                hash_set2 = self.hash_dict[path2]

                if hash_set1 == hash_set2:
                    continue

                if hash_set2.issubset(hash_set1):

                    contained_groups[path1].add(path2)

                    self.contained_views_to_remove.add(path2)

                elif hash_set1.issubset(hash_set2):

                    contained_groups[path2].add(path1)

                    self.contained_views_to_remove.add(path1)

                for k, v in contained_groups.items():
                    if path1 in v:
                        # view k contains view1, so it also contains any view that's contained in view1
                        if path1 in contained_groups.keys():
                            contained_groups[k].update(contained_groups[path1])
                    if path2 in v:
                        # view k contains view2, so it also contains any view that's contained in view2
                        if path2 in contained_groups.keys():
                            contained_groups[k].update(contained_groups[path2])

            i += 1

        return contained_groups

        # print(contained_groups)

        # contained_lists = []
        # for path1, paths in contained_groups.items():
        #     paths.add(path1)
        #     lst = list(paths)
        #     len_dict = {}
        #     for path in lst:
        #         len_dict[path] = len(self.path_to_df_dict[path])
        #     sorted_lst = sorted(lst, key=lambda path: len_dict[path], reverse=True)
        #
        #     contained_lists.append(sorted_lst)
        #
        #     self.contained_views_to_remove.update(set(sorted_lst[1:]))

        # print(contained_lists)

        # idx_to_remove = set()
        # for i in range(len(contained_lists)):
        #     lst1 = contained_lists[i]
        #     for j in range(len(contained_lists)):
        #         if i != j:
        #             lst2 = contained_lists[j]
        #             if set(lst1).issubset(lst2):
        #                 idx_to_remove.add(i)
        #             elif set(lst2).issubset(lst1):
        #                 idx_to_remove.add(j)
        # for idx in sorted(idx_to_remove, reverse=True):
        #     del contained_lists[idx]

        # print(contained_lists)

        # return contained_lists

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

        if self.found_contradictory_views:
            self._remove_from_contra_compl(self.contained_views_to_remove)

        return views_left

    def _remove_from_contra_compl(self, lst):

        # print(self.contradictions)

        contradictions_to_remove = set()
        complementary_idx_to_remove = set()

        for path in lst:
            for path1, path2 in self.contradictions.keys():
                if path == path1 or path == path2:
                    if (path1, path2) in self.contradictions.keys():
                        contradictions_to_remove.add((path1, path2))
                    elif (path2, path1) in self.contradictions.keys():
                        contradictions_to_remove.add((path2, path1))
            for i in range(len(self.complementary_pairs)):
                path1, path2, key = self.complementary_pairs[i]
                if path == path1 or path == path2:
                    complementary_idx_to_remove.add(i)

        # print(contradictions_to_remove)

        for path1, path2 in contradictions_to_remove:
            del self.contradictions[(path1, path2)]
        for i in sorted(complementary_idx_to_remove, reverse=True):
            del self.complementary_pairs[i]

    def find_contradictory_views(self):

        self.found_contradictory_views = True

        contradictions = defaultdict(lambda: defaultdict(set))
        for key, dfs in self.dfs_per_schema.items():
            cur_contradictions = self._find_contradictory_views(dfs)
            contradictions.update(cur_contradictions)

        def default_to_regular(d):
            if isinstance(d, defaultdict):
                d = {k: default_to_regular(v) for k, v in d.items()}
            return d

        contradictions = default_to_regular(contradictions)
        self.contradictions = contradictions

        return contradictions

    def _find_contradictory_views(self, dfs):

        candidate_key_to_inverted_index, view_to_candidate_keys_dict, find_candidate_keys_time = \
            build_inverted_index(
                dfs,
                self.candidate_key_size,
                self.uniqueness_threshold)

        start_time = time.time()

        contractions = defaultdict(lambda: defaultdict(set))

        already_classified_as_contradictory = set()
        complementary_pairs = set()

        for candidate_key, inverted_index in candidate_key_to_inverted_index.items():

            for key_value, dfs in inverted_index.items():
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

                                contractions[(path1, path2)][candidate_key].add(key_value)

                                already_added.add((path1, path2))
                                already_added.add((path2, path1))

                                already_classified_as_contradictory.add((path1, path2, candidate_key))
                                already_classified_as_contradictory.add((path2, path1, candidate_key))

        self.complementary_pairs += list(complementary_pairs - already_classified_as_contradictory)

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

        d = defaultdict(set)
        for view1, view2, key in self.complementary_pairs:
            d[(view1, view2)].add(key)

        return d

    def union_complementary_views(self):

        self.find_complementary_views()

        already_processed = set()

        for path1, path2, candidate_key in self.complementary_pairs:

            skip = False
            for pair in self.contradictions.keys():
                if path1 in pair or path2 in pair:
                    # only union when neither of the views have ANY contradiction with any other views
                    skip = True
            if skip:
                continue

            # if (path1, path2) in self.contradictions.keys() or (path2, path1) in self.contradictions.keys():
            # only union the two views when there is no contradiction for ANY key
            # continue

            # if (path1, path2) in already_processed:
            #     continue
            if path1 in already_processed or path2 in already_processed:
                continue

            df1 = self.path_to_df_dict[path1]
            df2 = self.path_to_df_dict[path2]

            new_df = pd.concat([df1, df2]).drop_duplicates().reset_index(drop=True)
            file_name = f"{os.path.splitext(path1)[0]}_union_{path2}"
            # if self.path_to_views is not None:
            #     new_path = os.path.join(self.path_to_views, file_name)
            #     new_df.to_csv(new_path)

            # already_processed.add((path1, path2))
            already_processed.add(path1)
            already_processed.add(path2)

            self.complementary_views_to_remove.add(path1)
            self.complementary_views_to_remove.add(path2)

            self.unioned_complementary_views.append((file_name, new_df))

        views_left = []
        for key, dfs in self.dfs_per_schema.items():
            new_dfs = []
            for df, path in dfs:
                if path not in self.complementary_views_to_remove:
                    new_dfs.append((df, path))
                    views_left.append(path)

            self.dfs_per_schema[key] = new_dfs

        for path, df in self.unioned_complementary_views:
            the_hashes = [hash(el) for el in df.columns]
            schema_id = sum(the_hashes)
            self.dfs_per_schema[schema_id].append((df, path))

            self.path_to_df_dict[path] = df

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

            for k1, k2 in self.contradictions.items():

                for key_tuple, key_values in self.contradictions[k1].items():

                    path1, path2 = k1

                    out.clear_output()

                    skip = False
                    if path1 in views_pruned or path2 in views_pruned or key_tuple in wrong_keys:
                        num_interactions += 1
                        skip = True

                    if num_interactions >= len(self.contradictions):
                        # we have explored all the contradictory / complementary view pairs and single views at least
                        # once
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
    # vd = ViewDistillation("/Users/zhiruzhu/Desktop/Niffler/ver/view_distillation/dataset/toytest/")
    vd = ViewDistillation(path_to_views="/Users/zhiruzhu/Desktop/Niffler/ver/output/")

    # res = vd.distill_views()
    # print(res)
    #
    # print(vd.contradictions)
    #
    vd.generate_graph()
    vd.prune_graph()

    # print("view3 -> view4:", vd.get_attributes("view3.csv", "view4.csv"))
