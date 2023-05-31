from aurum_api.algebra import AurumAPI
from aurum_api.apiutils import Relation
from collections import defaultdict
from collections import OrderedDict
import itertools
from archived.DoD.colors import Colors
from archived.DoD import data_processing_utils as dpu
from archived.DoD import material_view_analysis as mva
# from archived.DoD import FilterType
import numpy as np
from functools import reduce
import operator
import pickle
from knowledgerepr import fieldnetwork
from archived.modelstore import StoreHandler
import time
from archived.DoD import view_search_4c as v4c
import pprint
import server_config as config
from archived.DoD import column_infer
from enum import Enum

from tabulate import tabulate


pp = pprint.PrettyPrinter(indent=4)


class RelationType(Enum):
    ONE_ONE = 1
    ONE_MANY = 2
    MANY_ONE = 2
    MANY_MANY = 3


class ViewSearchPruning:

    def __init__(self, network, store_client, base_path, csv_separator=","):
        self.aurum_api = AurumAPI(network=network, store_client=store_client)
        self.base_path = base_path
        self.paths_cache = dict()
        self.view_cache = dict()
        dpu.configure_csv_separator(csv_separator)

    def clear_all_cache(self):
        self.paths_cache.clear()
        self.view_cache.clear()

    def place_paths_in_cache(self, t1, t2, paths):
        self.paths_cache[(t1, t2)] = paths
        self.paths_cache[(t2, t1)] = paths

    def are_paths_in_cache(self, t1, t2):
        if (t1, t2) in self.paths_cache:
            print("HIT!")
            return self.paths_cache[(t1, t2)]
        elif (t2, t1) in self.paths_cache:
            print("HIT!")
            return self.paths_cache[(t2, t1)]
        else:
            return None

    def individual_filters(self, sch_def):
        # Obtain sets that fulfill individual filters
        filter_drs = dict()
        filter_id = 0
        for attr in sch_def.keys():
            drs = self.aurum_api.search_exact_attribute(attr, max_results=200)
            filter_drs[(attr, FilterType.ATTR, filter_id)] = drs
            filter_id += 1

        for cell in sch_def.values():
            drs = self.aurum_api.search_content(cell, max_results=200)
            filter_drs[(cell, FilterType.CELL, filter_id)] = drs
            filter_id += 1
        return filter_drs

    def clusters2Hits(self, clusters, selected_index):
        hits = []
        for idx in selected_index:
            for row in clusters[idx]["data"]:
                hits.append(self.aurum_api._nid_to_hit(int(row[0])))
        return hits

    def virtual_schema_iterative_search2(self, list_samples, filter_drs, perf_stats, max_hops=2,
                                        debug_enumerate_all_jps=False, offset=10):
        table_fulfilled_filters = defaultdict(list)
        table_nid = dict()  # collect nids -- used later to obtain an access path to the tables
        # table_hits = defaultdict(list)
        for filter, hits in filter_drs.items():
            for hit in hits:
                table = hit.source_name
                nid = hit.nid
                table_nid[table] = nid
                if filter[2] not in [id for _, _, id in table_fulfilled_filters[table]]:
                    table_fulfilled_filters[table].append(((filter[0], hit.field_name), FilterType.ATTR, filter[2]))
                    # if len(table_fulfilled_filters[table]) == len(filter_drs):
                    #     table_fulfilled_filters.pop(table)
                    # table_hits[table].append(nid)

        table_path = obtain_table_paths(table_nid, self)

        # sort by value len -> # fulfilling filters
        table_fulfilled_filters = OrderedDict(
            sorted(table_fulfilled_filters.items(), key=lambda el:
            (len({filter_id for _, _, filter_id in el[1]}), el[0]), reverse=True))  # len of unique filters, then lexico

        # Ordering filters for more determinism
        for k, v in table_fulfilled_filters.items():
            v = sorted(v, key=lambda el: (el[2], el[0][0]), reverse=True)  # sort by id, then filter_name
            table_fulfilled_filters[k] = v

        def eager_candidate_exploration():
            def covers_filters(candidate_filters):
                candidate_filters_set = set([id for _, _, id in candidate_filters])
                if len(candidate_filters_set) == len(filter_drs.keys()):
                    return True
                return False

            def compute_size_filter_ix(filters, candidate_group_filters_covered):
                new_fs_set = set([id for _, _, id in filters])
                candidate_fs_set = set([id for _, _, id in candidate_group_filters_covered])
                ix_size = len(new_fs_set.union(candidate_fs_set)) - len(candidate_fs_set)
                return ix_size

            def clear_state():
                candidate_group_unordered.clear()
                candidate_group_filters_covered.clear()

            def sort_candidate_group(unordered_cg):
                ordered_cg = sorted(unordered_cg, key=lambda tup: tup[0])
                return [x[1] for x in ordered_cg]

            # Eagerly obtain groups of tables that cover as many filters as possible
            backup = []
            go_on = True
            while go_on:
                candidate_group_unordered = []
                candidate_group_filters_covered = set()
                for i in range(len(list(table_fulfilled_filters.items()))):
                    table_pivot, filters_pivot = list(table_fulfilled_filters.items())[i]
                    # Eagerly add pivot
                    candidate_group_unordered.append((filters_pivot[0][2],
                                                      table_pivot))  # (the largest filter_id, table_name) - add id for further sorting
                    candidate_group_filters_covered.update(filters_pivot)
                    # Did it cover all filters?
                    # if len(candidate_group_filters_covered) == len(filter_drs.items()):
                    if covers_filters(candidate_group_filters_covered):
                        candidate_group = sort_candidate_group(candidate_group_unordered)
                        # print("1: " + str(table_pivot))
                        yield (candidate_group, candidate_group_filters_covered)  # early stop
                        # Cleaning
                        clear_state()
                        continue
                    for j in range(len(list(table_fulfilled_filters.items()))):
                        idx = i + j + 1
                        if idx == len(table_fulfilled_filters.items()):
                            break
                        table, filters = list(table_fulfilled_filters.items())[idx]
                        # new_filters = len(set(filters).union(candidate_group_filters_covered)) - len(candidate_group_filters_covered)
                        new_filters = compute_size_filter_ix(filters, candidate_group_filters_covered)
                        if new_filters > 0:  # add table only if it adds new filters
                            candidate_group_unordered.append((filters[0][2], table))
                            candidate_group_filters_covered.update(filters)
                            if covers_filters(candidate_group_filters_covered):
                                candidate_group = sort_candidate_group(candidate_group_unordered)
                                # print("2: " + str(table_pivot))
                                yield (candidate_group, candidate_group_filters_covered)
                                clear_state()
                                # Re-add the current pivot, only necessary in this case
                                candidate_group_unordered.append((filters_pivot[0][2], table_pivot))
                                candidate_group_filters_covered.update(filters_pivot)
                    candidate_group = sort_candidate_group(candidate_group_unordered)
                    # print("3: " + str(table_pivot))
                    if covers_filters(candidate_group_filters_covered):
                        yield (candidate_group, candidate_group_filters_covered)
                    else:
                        backup.append(([el for el in candidate_group],
                                       set([el for el in candidate_group_filters_covered])))
                    # Cleaning
                    clear_state()
                # before exiting, return backup in case that may be useful
                # for candidate_group, candidate_group_filters_covered in backup:
                #     yield (candidate_group, candidate_group_filters_covered)
                go_on = False  # finished exploring all groups

        # Find ways of joining together each group
        cache_unjoinable_pairs = defaultdict(int)
        perf_stats['time_joinable'] = 0
        perf_stats['time_is_materializable'] = 0
        perf_stats['time_materialize'] = 0
        num_candidate_groups = 0
        all_join_graphs = []
        unique_all_join_graphs = set()
        unique_all_candidate_groups = set()
        all_filters = []
        for candidate_group, candidate_group_filters_covered in eager_candidate_exploration():
            num_candidate_groups += 1
            print("")
            print("Candidate group: " + str(candidate_group))
            num_unique_filters = len({f_id for _, _, f_id in candidate_group_filters_covered})
            print("Covers #Filters: " + str(num_unique_filters))

            if len(candidate_group) == 1:
                continue  # to go to the next group

            # Pre-check
            # TODO: with a connected components index we can pre-filter many of those groups without checking
            # group_with_all_relations, join_path_groups = self.joinable(candidate_group, cache_unjoinable_pairs)
            max_hops = max_hops
            # We find the different join graphs that would join the candidate_group
            join_graphs = self.joinable(candidate_group, cache_unjoinable_pairs, max_hops)
            print("Total join graphs:", len(join_graphs))
            all_join_graphs.extend(join_graphs)
        print(num_candidate_groups, len(all_join_graphs))
        return num_candidate_groups, len(all_join_graphs)

    def count_joins(self, list_samples, filter_drs, perf_stats, max_hops=2, debug_enumerate_all_jps=False, offset=10, dod_rank=False, materialize=True):
        st_stage2 = time.time()
        start = time.time()
        # We group now into groups that convey multiple filters.
        # Obtain list of tables ordered from more to fewer filters.
        table_fulfilled_filters = defaultdict(list)
        filter_fulfilled_tables = defaultdict(list)
        table_nid = dict()  # collect nids -- used later to obtain an access path to the tables

        for filter, hits in filter_drs.items():
            for hit in hits:
                table = hit.source_name
                nid = hit.nid
                table_nid[table] = nid
                filter_fulfilled_tables[filter].append((table, hit.field_name))
                if filter[2] not in [id for _, _, id in table_fulfilled_filters[table]]:
                    table_fulfilled_filters[table].append(((filter[0], hit.field_name), FilterType.ATTR, filter[2]))

        table_path = obtain_table_paths(table_nid, self)

        # sort by value len -> # fulfilling filters
        table_fulfilled_filters = OrderedDict(
            sorted(table_fulfilled_filters.items(), key=lambda el:
            (len({filter_id for _, _, filter_id in el[1]}), el[0]), reverse=True))  # len of unique filters, then lexico

        # Ordering filters for more determinism
        for k, v in table_fulfilled_filters.items():
            v = sorted(v, key=lambda el: (el[2], el[0][0]), reverse=True)  # sort by id, then filter_name
            table_fulfilled_filters[k] = v

        def eager_candidate_exploration():
            print("start eager exploration")
            candidate_table_groups = filter_fulfilled_tables.values()
            filters = list(filter_drs.keys())
            import itertools
            # print(list(itertools.product(*candidate_table_groups)))
            for group in list(itertools.product(*candidate_table_groups)):
                candidate_group_unordered = []
                candidate_group_filters_covered = []
                for idx, item in enumerate(group):
                    if item[0] not in candidate_group_unordered:
                        candidate_group_unordered.append(item[0])
                    f = ((filters[idx][0], item[1]), FilterType.ATTR, filters[idx][2])
                    if f not in candidate_group_filters_covered:
                        candidate_group_filters_covered.append(f)
                yield (list(candidate_group_unordered), candidate_group_filters_covered)

        et_stage2 = time.time()
        perf_stats['t_stage2'] = (et_stage2 - st_stage2)
        # Find ways of joining together each group
        cache_unjoinable_pairs = defaultdict(int)
        perf_stats['time_joinable'] = 0
        perf_stats['time_is_materializable'] = 0
        perf_stats['time_materialize'] = 0
        num_candidate_groups = 0
        all_join_graphs = []
        all_filters = []
        unique_all_join_graphs = set()
        unique_all_candidate_groups = set()
        for candidate_group, candidate_group_filters_covered in eager_candidate_exploration():
            unique_all_candidate_groups.add(tuple(candidate_group))
            num_candidate_groups += 1
            print("Candidate group: " + str(candidate_group))
            num_unique_filters = len({f_id for _, _, f_id in candidate_group_filters_covered})
            print("Covers #Filters: " + str(num_unique_filters))

            if len(candidate_group) == 1:
                continue  # to go to the next group

            max_hops = max_hops
            # We find the different join graphs that would join the candidate_group
            st_joinable = time.time()
            join_graphs = self.joinable(candidate_group, cache_unjoinable_pairs, max_hops)
            et_joinable = time.time()
            print("Total join graphs:", len(join_graphs))

            # if not graphs skip next
            if len(join_graphs) == 0:
                if 'unjoinable_candidate_group' not in perf_stats:
                    perf_stats['unjoinable_candidate_group'] = 0
                perf_stats['unjoinable_candidate_group'] += 1
                print("Group: " + str(candidate_group) + " is Non-Joinable with max_hops=" + str(max_hops))
                continue
            if 'joinable_candidate_group' not in perf_stats:
                perf_stats['joinable_candidate_group'] = 0
            perf_stats['joinable_candidate_group'] += 1
            if 'num_join_graphs_per_candidate_group' not in perf_stats:
                perf_stats['num_join_graphs_per_candidate_group'] = []
            perf_stats['num_join_graphs_per_candidate_group'].append(len(join_graphs))

            # Now we need to check every join graph individually and see if it's materializable. Only once we've
            # exhausted these join graphs we move on to the next candidate group. We know already that each of the
            # join graphs covers all tables in candidate_group, so if they're materializable we're good.
            total_materializable_join_graphs = 0
            materializable_join_graphs = []
            filters = candidate_group_filters_covered
            for jpg in join_graphs:
                total_materializable_join_graphs += 1
                materializable_join_graphs.append((jpg, filters))
                all_join_graphs.append(jpg)
                unique_all_join_graphs.add(tuple(jpg))
                all_filters.append(filters)

        perf_stats["num_candidate_groups"] = len(unique_all_candidate_groups)
        perf_stats["num_join_graphs"] = len(unique_all_join_graphs)
        print("Finished enumerating groups")
        return len(unique_all_candidate_groups), len(unique_all_join_graphs)

    def search_views(self, list_samples, filter_drs, perf_stats, max_hops=2, debug_enumerate_all_jps=False, offset=10, dod_rank=False, materialize=True):
        st_stage2 = time.time()
        start = time.time()
        # We group now into groups that convey multiple filters.
        # Obtain list of tables ordered from more to fewer filters.
        table_fulfilled_filters = defaultdict(list)
        filter_fulfilled_tables = defaultdict(list)
        table_nid = dict()  # collect nids -- used later to obtain an access path to the tables

        for filter, hits in filter_drs.items():
            for hit in hits:
                table = hit.source_name
                nid = hit.nid
                table_nid[table] = nid
                filter_fulfilled_tables[filter].append((table, hit.field_name))
                if filter[2] not in [id for _, _, id in table_fulfilled_filters[table]]:
                    table_fulfilled_filters[table].append(((filter[0], hit.field_name), FilterType.ATTR, filter[2]))


        table_path = obtain_table_paths(table_nid, self)

        # sort by value len -> # fulfilling filters
        table_fulfilled_filters = OrderedDict(
            sorted(table_fulfilled_filters.items(), key=lambda el:
            (len({filter_id for _, _, filter_id in el[1]}), el[0]), reverse=True))  # len of unique filters, then lexico

        # Ordering filters for more determinism
        for k, v in table_fulfilled_filters.items():
            v = sorted(v, key=lambda el: (el[2], el[0][0]), reverse=True)  # sort by id, then filter_name
            table_fulfilled_filters[k] = v

        def eager_candidate_exploration():
            print("start eager exploration")
            candidate_table_groups = filter_fulfilled_tables.values()
            filters = list(filter_drs.keys())
            import itertools
            # print(list(itertools.product(*candidate_table_groups)))
            for group in list(itertools.product(*candidate_table_groups)):
                candidate_group_unordered = []
                candidate_group_filters_covered = []
                for idx, item in enumerate(group):
                    if item[0] not in candidate_group_unordered:
                        candidate_group_unordered.append(item[0])
                    f = ((filters[idx][0], item[1]), FilterType.ATTR, filters[idx][2])
                    if f not in candidate_group_filters_covered:
                        candidate_group_filters_covered.append(f)
                yield (list(candidate_group_unordered), candidate_group_filters_covered)


        et_stage2 = time.time()
        perf_stats['t_stage2'] = (et_stage2 - st_stage2)
        # Find ways of joining together each group
        cache_unjoinable_pairs = defaultdict(int)
        perf_stats['time_joinable'] = 0
        perf_stats['time_is_materializable'] = 0
        perf_stats['time_materialize'] = 0
        num_candidate_groups = 0
        all_join_graphs = []
        all_filters = []
        unique_all_join_graphs = set()
        unique_all_candidate_groups = set()
        for candidate_group, candidate_group_filters_covered in eager_candidate_exploration():
            unique_all_candidate_groups.add(tuple(candidate_group))
            num_candidate_groups += 1
            print("")
            print("Candidate group: " + str(candidate_group))
            num_unique_filters = len({f_id for _, _, f_id in candidate_group_filters_covered})
            print("Covers #Filters: " + str(num_unique_filters))

            if len(candidate_group) == 1:
                table = candidate_group[0]
                path = table_path[table]
                # materialized_virtual_schema = dpu.get_dataframe(path + "/" + table)
                if table in self.view_cache:
                    materialized_virtual_schema = self.view_cache[table]
                    print("HIT Cached View")
                else:
                    materialized_virtual_schema = dpu.read_relation(path + "/" + table)
                    self.view_cache[table] = materialized_virtual_schema
                attrs_to_project = dpu.obtain_attributes_to_project(candidate_group_filters_covered)
                # Create metadata to document this view
                view_metadata = dict()
                view_metadata["#join_graphs"] = 1
                view_metadata["join_graph"] = {"nodes": [{"id": -101010, "label": table}], "edges": []}
                if 'single_relation_group' not in perf_stats:
                    perf_stats['single_relation_group'] = 0
                perf_stats['single_relation_group'] += 1
                yield materialized_virtual_schema, attrs_to_project, view_metadata, table, 1, 1
                continue  # to go to the next group

            # Pre-check
            # TODO: with a connected components index we can pre-filter many of those groups without checking
            #group_with_all_relations, join_path_groups = self.joinable(candidate_group, cache_unjoinable_pairs)
            max_hops = max_hops
            # We find the different join graphs that would join the candidate_group
            st_joinable = time.time()
            join_graphs = self.joinable(candidate_group, cache_unjoinable_pairs, max_hops)
            et_joinable = time.time()
            print("Total join graphs:",len(join_graphs))
            perf_stats['time_joinable'] += (et_joinable - st_joinable)
            if debug_enumerate_all_jps:
                for i, group in enumerate(join_graphs):
                    print("Group: " + str(i))
                    for el in group:
                        print(el)
                continue  # We are just interested in all JPs for all candidate groups

            # if not graphs skip next
            if len(join_graphs) == 0:
                if 'unjoinable_candidate_group' not in perf_stats:
                    perf_stats['unjoinable_candidate_group'] = 0
                perf_stats['unjoinable_candidate_group'] += 1
                print("Group: " + str(candidate_group) + " is Non-Joinable with max_hops=" + str(max_hops))
                continue
            if 'joinable_candidate_group' not in perf_stats:
                perf_stats['joinable_candidate_group'] = 0
            perf_stats['joinable_candidate_group'] += 1
            if 'num_join_graphs_per_candidate_group' not in perf_stats:
                perf_stats['num_join_graphs_per_candidate_group'] = []
            perf_stats['num_join_graphs_per_candidate_group'].append(len(join_graphs))

            # Now we need to check every join graph individually and see if it's materializable. Only once we've
            # exhausted these join graphs we move on to the next candidate group. We know already that each of the
            # join graphs covers all tables in candidate_group, so if they're materializable we're good.
            total_materializable_join_graphs = 0
            materializable_join_graphs = []
            filters = candidate_group_filters_covered
            for jpg in join_graphs:
                total_materializable_join_graphs += 1
                materializable_join_graphs.append((jpg, filters))
                all_join_graphs.append(jpg)
                unique_all_join_graphs.add(tuple(jpg))
                all_filters.append(filters)


        perf_stats["num_candidate_groups"] = len(unique_all_candidate_groups)
        perf_stats["num_join_graphs"] = len(unique_all_join_graphs)
        print("Finished enumerating groups")

        if not materialize:
            return perf_stats
        # Rate all join paths after pruning
        # print("total join graphs", len(all_join_graphs))

        # table_paths = {}
        # # build inverted index candidate tables -> [indexes of corresponding join paths]
        # for idx, jpg in enumerate(all_join_graphs):
        #     table_list = []
        #     table_hash = {}
        #     for l, r in jpg:
        #         if l.source_name not in table_hash.keys():
        #             table_list.append(l.source_name)
        #             table_hash[l.source_name] = True
        #         if r.source_name not in table_hash.keys():
        #             table_list.append(r.source_name)
        #             table_hash[r.source_name] = True
        #     if tuple(table_list) not in table_paths.keys():
        #         table_paths[tuple(table_list)] = [idx]
        #     else:
        #         table_paths[tuple(table_list)].append(idx)
        '''
        main scoring logic
        '''
        if dod_rank:
            ranking_start = time.time()
            score_list = []
            for idx, path in enumerate(all_join_graphs):
                score = 0
                # threshold = 0.8
                relations = set()
                for l, r in path:
                    relations.add(l.source_name)
                    relations.add(r.source_name)
                    unique_score = max(self.aurum_api.helper.get_uniqueness_score(l.nid),
                                       self.aurum_api.helper.get_uniqueness_score(r.nid))
                    score += unique_score
                    # if unique_score > threshold:
                    #     score += 1
                score = score / len(path) / (1 + np.log(1 + np.log(len(relations))))  # add a penalty to the number of involved relations
                score_list.append((score, idx))
            score_list.sort(reverse=True)
            sorted_all_graphs = [(all_join_graphs[x[1]], all_filters[x[1]], x[0]) for x in score_list]
            paths_to_materialize = sorted_all_graphs[0: offset]
            perf_stats["ranking_time"] = time.time() - ranking_start
        else:
            paths_to_materialize = [(all_join_graphs[idx], all_filters[idx], 0) for idx in range(len(all_join_graphs))]
        to_return = self.materialize_join_graphs(list_samples, paths_to_materialize)
        perf_stats["search_time"] = time.time() - start
        for el in to_return:
            yield el

    def not_zero_view(self, join_path):
        '''
        if this join path will produce a view that has more than 0 row
        :param join_path:
        :return:
        '''
        if len(join_path) < 2:
            # we have guaranteed that a pair of joinable columns has overlap
            return True
        join_keys = []
        for l, r in path:
            join_keys.append((l.source_name, l.field_name))
            join_keys.append((r.source_name, r.field_name))

        df_l = dpu.read_column(self.base_path + join_keys[0][0], join_keys[0][1])[join_keys[0][1]].dropna().drop_duplicates().values.tolist()
        df_r = dpu.read_column(self.base_path + join_keys[-1][0], join_keys[-1][1])[join_keys[-1][1]].dropna().drop_duplicates().values.tolist()
        lookup_l = set(df_l)
        lookup_r = set(df_r)
        df_mid = dpu.read_columns(self.base_path + join_keys[1][0], [join_keys[1][1], join_keys[2][1]])

        return True

    def get_relation(self, df, join_key, target_cols, sampling_fraction):
        df.dropna()
        df.sample(frac=sampling_fraction, replace=True, random_state=1)
        first_max = df.groupby(join_key).count().max()[0]
        if len(target_cols) == 1:
            second_max = df.groupby(target_cols[0]).count().max()[0]
        else:
            second_max = df.groupby(target_cols).size().groupby(level=1).max()
            if len(second_max) > 0:
                second_max = second_max[0]
            else:
                return 4
        if first_max == 1:
            if second_max == 1:
                return RelationType.ONE_ONE.value
            else:
                return RelationType.ONE_MANY.value
        else:
            if second_max == 1:
                return RelationType.MANY_ONE.value
            else:
                return RelationType.MANY_MANY.value

    def is_compatible_valid(self, group):
        view_hash = {}
        valid_list = []
        for item in group:
            view = item.split(";")
            sorted_view = sorted(view)
            if tuple(sorted_view) not in view_hash.keys():
                valid_list.append(view)
                view_hash[tuple(sorted_view)] = True
        if len(valid_list) > 1:
            return True
        else:
            return False

    def prune_join_paths(self, all_join_graphs, table_fulfilled_filters, table_path, tk_cache, tk_info, index):
        tk_views = []
        tk_hash = {}
        for idx, flat_jpg in enumerate(all_join_graphs):
            if index >= len(flat_jpg):
                continue
            join_key = []
            if index == 0 or index == len(flat_jpg)-1:
                join_key.append(flat_jpg[index])
                table_name = join_key[0].source_name
            else:
                join_key_1 = flat_jpg[index]
                table_name = join_key_1.source_name
                join_key.append(join_key_1)
                join_key_2 = flat_jpg[index+1]
                if join_key_1.source_name == join_key_2.source_name and join_key_1.field_name != join_key_2.field_name:
                    join_key.append(join_key_2)
                elif join_key_1.source_name != join_key_2.source_name:
                    continue
            if table_name not in table_fulfilled_filters.keys():
                continue
            target_cols = table_fulfilled_filters[table_name]
            tk = []
            lookup = {}
            target = []
            join_key_set = []

            for key in join_key:
                join_key_set.append(key.field_name)
                lookup[key.field_name] = True

            for x in target_cols:
                if x[0][1] not in lookup.keys():
                    target.append(x[0][1])
                    lookup[x[0][1]] = True

            tk_info[idx].append((target, join_key_set))
            tk.extend(target)
            tk.extend(join_key_set)

            if tuple(tk) in tk_hash.keys():
                tk_hash[tuple(tk)].append(idx)
                continue
            tk_hash[tuple(tk)] = [idx]
            # print(table_name, tk)
            tk_view = self.columns_to_view(table_path[table_name] + table_name, tk)
            tk_cache[tuple(tk)] = tk_view
            if len(tk_view) > 0:
                tk_views.append((tk_view, self.generate_view_name(tk)))
        return tk_views, tk_hash

    def get_dup_idx(self, tk_views, tk_hash):
        groups_per_column_cardinality = v4c.perform4c(tk_views)

        paths_to_remove = []
        for k, v in groups_per_column_cardinality.items():
            compatible_group = v['compatible']
            for group in compatible_group:
                selected_view = group[0].split(";")
                paths_to_remove.extend(tk_hash[tuple(selected_view)])

            print("Compatible groups:", str(len(compatible_group)))
            for group in compatible_group:
                print(group)
        return paths_to_remove


    def columns_to_view(self, source, columns):
        # input: columns from one table
        # output: view drs
        df = dpu.read_columns(source, columns)
        df = mva.curate_view(df)
        df = v4c.normalize(df)
        return df

    def generate_view_name(self, columns):
        name = ""
        for col in columns:
            name += col + ";"
        return name[0:-1]

    def compute_join_graph_id(self, join_graph):
        all_nids = []
        for hop_l, hop_r in join_graph:
            all_nids.append(hop_r.nid)
            all_nids.append(hop_l.nid)
        path_id = sum([hash(el) for el in all_nids])
        return path_id

    def __compute_join_graph_id(self, join_graph):
        all_nids = []
        for hop_l, hop_r in join_graph:
            all_nids.append(hop_r.nid)
            all_nids.append(hop_l.nid)
        path_id = frozenset(all_nids)
        return path_id

    def materialize_join_graphs(self, samples, materializable_join_graphs):
        to_return = []
        idx = 0
        for mjg, filters, jp_score in materializable_join_graphs:
            # if is_join_graph_valid:
            attrs_to_project = dpu.obtain_attributes_to_project(filters)

            idx += 1

            if tuple(mjg) in self.view_cache:
                materialized_virtual_schema = self.view_cache[tuple(mjg)]
                print("HIT Cached View")
            else:
                materialized_virtual_schema = dpu.materialize_join_graph_sample(mjg, samples, filters, self, idx,
                                                                                sample_size=2000)
                self.view_cache[tuple(mjg)] = materialized_virtual_schema
            join_path = ""
            relations = set()
            if materialized_virtual_schema is False:
                # print(mjg)
                continue  # happens when the join was an outlier
            else:
                for l, r in mjg:
                    relations.add(l.source_name)
                    relations.add(r.source_name)
                    join_path += l.source_name + "-" + l.field_name + " JOIN " + r.source_name + "-" + r.field_name + "; "
            # Create metadata to document this view
            view_metadata = dict()
            view_metadata["#join_graphs"] = len(materializable_join_graphs)
            # view_metadata["join_graph"] = self.format_join_paths_pairhops(jpg)
            view_metadata["join_graph"] = self.format_join_graph_into_nodes_edges(mjg)
            to_return.append((materialized_virtual_schema, attrs_to_project, view_metadata, join_path[:-2], len(relations), jp_score))
            # yield materialized_virtual_schema, attrs_to_project, view_metadata
        return to_return

    def joinable(self, group_tables: [str], cache_unjoinable_pairs: defaultdict(int), max_hops=2):
        """
        Find all join graphs that connect the tables in group_tables. This boils down to check
        whether there is (at least) a set of join paths which connect all tables, but it is required to find all
        possible join graphs and not only one.
        :param group_tables:
        :param cache_unjoinable_pairs: this set contains pairs of tables that do not join with each other
        :return:
        """
        assert len(group_tables) > 1

        # if not the size of group_tables, there won't be unique jps with all tables. that may not be good though
        max_hops = max_hops

        # for each pair of tables in group keep list of (path, tables_covered)
        paths_per_pair = defaultdict(list)

        table_combinations = [el for el in itertools.combinations(group_tables, 2)]

        for table1, table2 in table_combinations:
            # Check if tables are already known to be unjoinable
            if (table1, table2) in cache_unjoinable_pairs.keys() or (table2, table1) in cache_unjoinable_pairs.keys():
                continue  # FIXME FIXME FIXME
            t1 = self.aurum_api.make_drs(table1)
            t2 = self.aurum_api.make_drs(table2)
            t1.set_table_mode()
            t2.set_table_mode()
            # Check cache first, if not in cache then do the search
            # drs = self.are_paths_in_cache(table1, table2)
            paths = self.are_paths_in_cache(table1, table2)  # list of lists
            if paths is None:
                print("\nFinding paths between " + str(table1) + " and " + str(table2))
                print("max hops: " + str(max_hops))
                s = time.time()
                drs = self.aurum_api.paths(t1, t2, Relation.PKFK, max_hops=max_hops, lean_search=True)
                e = time.time()
                print("Total time: " + str((e-s)))
                paths = drs.paths()  # list of lists
                print("Total paths:", len(paths))
                self.place_paths_in_cache(table1, table2, paths)  # FIXME FIXME FIXME
            # paths = drs.paths()  # list of lists
            # If we didn't find paths, update unjoinable_pairs cache with this pair
            if len(paths) == 0:  # then store this info, these tables do not join # FIXME FIXME FIXME
                cache_unjoinable_pairs[(table1, table2)] += 1  # FIXME FIXME
                cache_unjoinable_pairs[(table2, table1)] += 1  # FIXME FIXME FIXME
            for p in paths:
                tables_covered = set()
                tables_in_group = set(group_tables)
                for hop in p:
                    if hop.source_name in tables_in_group:
                        # this is a table covered by this path
                        tables_covered.add(hop.source_name)
                paths_per_pair[(table1, table2)].append((p, tables_covered))

        if len(paths_per_pair) == 0:
            return []

        # enumerate all possible join graphs
        all_combinations = [el for el in itertools.product(*list(paths_per_pair.values()))]
        deduplicated_paths = dict()
        # Add combinations
        for path_combination in all_combinations:
            # TODO: is this general if max_hops > 2?
            for p1, p2 in itertools.combinations(path_combination, 2):
                path1, tables_covered1 = p1
                path2, tables_covered2 = p2
                # does combining these two paths help to cover more tables?
                if len(tables_covered1) > len(tables_covered2):
                    current_cover_len = len(tables_covered1)
                else:
                    current_cover_len = len(tables_covered2)
                potential_cover = tables_covered1.union(tables_covered2)
                joinable_paths = tables_covered1.intersection(tables_covered2)
                potential_cover_len = len(potential_cover)
                # if we cover more tables, and the paths are joinable (at least one table in common)
                if potential_cover_len > current_cover_len and len(joinable_paths) > 0:
                    # Transform paths into pair-hops so we can join them together
                    tx_path1 = self.transform_join_path_to_pair_hop(path1)
                    tx_path2 = self.transform_join_path_to_pair_hop(path2)
                    # combine the paths
                    combined_path = tx_path1 + tx_path2
                    path_id = self.compute_join_graph_id(combined_path)
                    # If I haven't generated this path elsewhere, then I add it along with the tables it covers
                    if path_id not in deduplicated_paths:
                        deduplicated_paths[path_id] = (combined_path, potential_cover)

        # Add initial paths that may cover all tables and remove those that do not
        for p, tables_covered in list(paths_per_pair.values())[0]:
            if len(tables_covered) == len(group_tables):
                tx_p = self.transform_join_path_to_pair_hop(p)
                path_id = self.compute_join_graph_id(tx_p)
                deduplicated_paths[path_id] = (tx_p, tables_covered)

        # Now we filter out all paths that do not cover all the tables in the group
        covering_join_graphs = [jg[0] for _, jg in deduplicated_paths.items() if len(jg[1]) == len(group_tables)]

        # Finally sort by number of required joins
        join_graphs = sorted(covering_join_graphs, key=lambda x: len(x))
        return join_graphs

    def format_join_graph_into_nodes_edges(self, join_graph):
        nodes = dict()
        edges = []
        for jp in join_graph:
            # Add nodes
            for hop in jp:
                label = hop.db_name + ":" + hop.source_name
                node_descr = {"id": hash(label), "label": label}  # cannot use nid cause that's for cols not rels
                node_id = hash(label)
                if node_id not in nodes:
                    nodes[node_id] = node_descr
            l, r = jp
            l_label = l.db_name + ":" + l.source_name
            r_label = r.db_name + ":" + r.source_name
            edge_descr = {"from": hash(l_label), "to": hash(r_label)}
            edges.append(edge_descr)
        return {"nodes": list(nodes.values()), "edges": list(edges)}

    def transform_join_path_to_pair_hop(self, join_path):
        """
        1. get join path, 2. annotate with values to check for, then 3. format into [(l,r)]
        :param join_paths:
        :param table_fulfilled_filters:
        :return:
        """
        jp_hops = []
        pair = []
        for hop in join_path:
            pair.append(hop)
            if len(pair) == 2:
                jp_hops.append(tuple(pair))
                pair.clear()
                pair.append(hop)
        # Now remove pairs with pointers within same relation, as we don't need to join these
        jp_hops = [(l, r) for l, r in jp_hops if l.source_name != r.source_name]
        return jp_hops

    def is_join_graph_materializable(self, join_graph, table_fulfilled_filters):
        # FIXME: add a way of collecting the join cardinalities and propagating them outside as well
        local_intermediates = dict()

        for l, r in join_graph:
            # apply filters to l
            if l.source_name not in local_intermediates:
                l_path = self.aurum_api.helper.get_path_nid(l.nid)
                # If there are filters apply them
                if l.source_name in table_fulfilled_filters:
                    filters_l = table_fulfilled_filters[l.source_name]
                    filtered_l = None
                    for info, filter_type, filter_id in filters_l:
                        if filter_type == FilterType.ATTR:
                            filtered_l = dpu.read_relation_on_copy(l_path + l.source_name)  # FIXME FIXME FIXME
                            # filtered_l = dpu.get_dataframe(l_path + l.source_name)
                            continue  # no need to filter anything if the filter is only attribute type
                        attribute = info[1]
                        cell_value = info[0]
                        filtered_l = dpu.apply_filter(l_path + l.source_name, attribute, cell_value)# FIXME FIXME FIXME

                    if len(filtered_l) == 0:
                        return False  # filter does not leave any data => non-joinable
                # If there are not filters, then do not apply them
                else:
                    filtered_l = dpu.read_relation_on_copy(l_path + l.source_name)# FIXME FIXME FIXME
                    # filtered_l = dpu.get_dataframe(l_path + l.source_name)
            else:
                filtered_l = local_intermediates[l.source_name]
            local_intermediates[l.source_name] = filtered_l

            # apply filters to r
            if r.source_name not in local_intermediates:
                r_path = self.aurum_api.helper.get_path_nid(r.nid)
                # If there are filters apply them
                if r.source_name in table_fulfilled_filters:
                    filters_r = table_fulfilled_filters[r.source_name]
                    filtered_r = None
                    for info, filter_type, filter_id in filters_r:
                        if filter_type == FilterType.ATTR:
                            filtered_r = dpu.read_relation_on_copy(r_path + r.source_name)# FIXME FIXME FIXME
                            # filtered_r = dpu.get_dataframe(r_path + r.source_name)
                            continue  # no need to filter anything if the filter is only attribute type
                        attribute = info[1]
                        cell_value = info[0]
                        filtered_r = dpu.apply_filter(r_path + r.source_name, attribute, cell_value)

                    if len(filtered_r) == 0:
                        return False  # filter does not leave any data => non-joinable
                # If there are not filters, then do not apply them
                else:
                    filtered_r = dpu.read_relation_on_copy(r_path + r.source_name)# FIXME FIXME FIXME
                    # filtered_r = dpu.get_dataframe(r_path + r.source_name)
            else:
                filtered_r = local_intermediates[r.source_name]
            local_intermediates[r.source_name] = filtered_r

            # check if the materialized version join's cardinality > 0
            joined = dpu.join_ab_on_key(filtered_l, filtered_r, l.field_name, r.field_name, suffix_str="_x")

            if len(joined) == 0:
                return False  # non-joinable hop enough to discard join graph
        # if we make it through all hopes, then join graph is materializable (i.e., verified)
        return True


def rank_materializable_join_graphs(materializable_join_paths, table_path, dod):

    def score_for_key(keys_score, target):
        for c, nunique, score in keys_score:
            if target == c:
                return score

    def aggr_avg(scores):
        scores = np.asarray(scores)
        return np.average(scores)

    def aggr_mul(scores):
        return reduce(operator.mul, scores)

    rank_jps = []
    keys_cache = dict()
    for mjp in materializable_join_paths:
        jump_scores = []
        for filter, l, r in mjp:
            table = l.source_name
            if table not in keys_cache:
                if table not in table_path:
                    nid = (dod.aurum_api.make_drs(table)).data[0].nid
                    path = dod.aurum_api.helper.get_path_nid(nid)
                    table_path[table] = path
                path = table_path[table]
                table_df = dpu.get_dataframe(path + "/" + table)
                likely_keys_sorted = mva.most_likely_key(table_df)
                keys_cache[table] = likely_keys_sorted
            likely_keys_sorted = keys_cache[table]
            jump_score = score_for_key(likely_keys_sorted, l.field_name)
            jump_scores.append(jump_score)
        jp_score_avg = aggr_avg(jump_scores)
        jp_score_mul = aggr_mul(jump_scores)
        rank_jps.append((mjp, jp_score_avg, jp_score_mul))
    rank_jps = sorted(rank_jps, key=lambda x: x[1], reverse=True)
    return rank_jps


def rank_materializable_join_paths_piece(materializable_join_paths, candidate_group, table_path, dod):
    # compute rank list of likely keys for each table
    table_keys = dict()
    table_field_rank = dict()
    for table in candidate_group:
        if table in table_path:
            path = table_path[table]
        else:
            nid = (dod.aurum_api.make_drs(table)).data[0].nid
            path = dod.aurum_api.helper.get_path_nid(nid)
            table_path[table] = path
        table_df = dpu.get_dataframe(path + "/" + table)
        likely_keys_sorted = mva.most_likely_key(table_df)
        table_keys[table] = likely_keys_sorted
        field_rank = {payload[0]: i for i, payload in enumerate(likely_keys_sorted)}
        table_field_rank[table] = field_rank

    # 1) Split join paths into its pairs, then 2) sort each pair individually, then 3) assemble again

    num_jumps = sorted([len(x) for x in materializable_join_paths])[-1]
    jump_joins = {i: [] for i in range(num_jumps)}

    # 1) split
    for annotated_jp in materializable_join_paths:
        for i, jp in enumerate(annotated_jp):
            jump_joins[i].append(jp)

    def field_to_rank(table, field):
        return table_field_rank[table][field]

    # 2) sort
    for jump, joins in jump_joins.items():
        joins = sorted(joins, key=lambda x: field_to_rank(x[1].source_name, x[1].field_name))
        jump_joins[jump] = joins

    # 3) assemble
    ranked_materialized_join_paths = [[] for _ in range(len(materializable_join_paths))]
    for jump, joins in jump_joins.items():
        for i, join in enumerate(joins):
            ranked_materialized_join_paths[i].append(join)

    return ranked_materialized_join_paths


def obtain_table_paths(set_nids, dod):
    table_path = dict()
    for table, nid in set_nids.items():
        path = dod.aurum_api.helper.get_path_nid(nid)
        table_path[table] = path
    return table_path

def found_view_eval(ci, attrs, values, gt_cols):
    candidate_columns, sample_score, hit_type_dict, match_dict, hit_dict = ci.infer_candidate_columns(attrs, values)
    results = []
    for _, columns in candidate_columns.items():
        results.append([(c.source_name, c.field_name) for c in columns])
    found1 = found_gt_view_or_not(results, gt_cols)

    results = ci.view_spec_benchmark(sample_score)
    found2 = found_gt_view_or_not(results, gt_cols)

    results, _, _ = ci.view_spec_cluster2(candidate_columns, sample_score)
    found3 = found_gt_view_or_not(results, gt_cols)

    return found1, found2, found3

def pipeline_evaluation(vs, ci, attrs, values, gt_cols, offset = 1000, output_path = ""):
    candidate_columns, sample_score, hit_type_dict, match_dict, hit_dict = ci.infer_candidate_columns(attrs, values)
    results = []
    filter_drs = {}
    perf_stats = dict()
    '''
    baseline 1: S4
    '''
    idx = 0
    for column, candidates in candidate_columns.items():
        results.append([(c.source_name, c.field_name) for c in candidates])
        filter_drs[(column, FilterType.ATTR, idx)] = candidates
        idx += 1
    found1 = found_gt_view_or_not(results, gt_cols)

    '''
    baseline2: SQUID
    '''
    results = ci.view_spec_benchmark(sample_score)
    for column, _ in candidate_columns.items():
        filter_drs[(column, FilterType.ATTR, idx)] = [hit_dict[x] for x in results[idx]]
    found2 = found_gt_view_or_not(results, gt_cols)

    '''
    baseline3: DoD
    '''
    results, _, _ = ci.view_spec_cluster2(candidate_columns, sample_score)
    found3 = found_gt_view_or_not(results, gt_cols)

def run_view_search(vs, filter_drs, perf_stats, flag, offset = 1000, output_path = ""):
    i = 0
    log = open(output_path + "/log.txt", "w")
    log.write("Method" + str(flag))
    for mjp, attrs_project, metadata, jp in vs.search_views({}, filter_drs, perf_stats, max_hops=2,
                                                            debug_enumerate_all_jps=False, offset=offset):
        log.write("view" + str(i))
        log.write(jp)
        log.write(attrs_project)
        proj_view = dpu.project(mjp, attrs_project)
        full_view = False

        if output_path is not None:
            if full_view:
                view_path = output_path + "/raw_view_" + str(i)
                mjp.to_csv(view_path, encoding='latin1', index=False)
            view_path = output_path + "/view_" + str(i) + ".csv"
            proj_view.to_csv(view_path, encoding='latin1', index=False)  # always store this

        i += 1
    print("total views:", i)


def found_gt_view_or_not(results, gt_cols):
    found = True
    for i, result in enumerate(results):
        if gt_cols[i] not in result:
            found = False
            break
    return found

def evaluate_view_search(vs, ci, attrs, values, flag, gt_cols, offset = 1000, output_path = ""):
    candidate_columns, sample_score, hit_type_dict, match_dict, hit_dict = ci.infer_candidate_columns(attrs, values)
    filter_drs = {}
    perf_stats = dict()
    results = []

    if flag == 1:
        idx = 0
        for column, candidates in candidate_columns.items():
            results.append([(c.source_name, c.field_name) for c in candidates])
            filter_drs[(column, FilterType.ATTR, idx)] = candidates
            idx += 1
        found = found_gt_view_or_not(results, gt_cols)
    elif flag == 2:
        results = ci.view_spec_benchmark(sample_score)
        idx = 0
        for column, _ in candidate_columns.items():
            filter_drs[(column, FilterType.ATTR, idx)] = [hit_dict[x] for x in results[idx]]
            idx += 1
    elif flag == 3:
        results, _, results_hit = ci.view_spec_cluster2(candidate_columns, sample_score)
        idx = 0
        for column, _ in candidate_columns.items():
            filter_drs[(column, FilterType.ATTR, idx)] = results_hit[idx]
            idx += 1

    i = 0
    log = open(output_path + "/log.txt", "w")
    log.write("Method" + str(flag))
    for mjp, attrs_project, metadata, jp in vs.search_views({}, filter_drs, perf_stats, max_hops=2,
                                       debug_enumerate_all_jps=False, offset=offset):
        log.write("view" + str(i))
        log.write(jp)
        log.write(attrs_project)
        proj_view = dpu.project(mjp, attrs_project)
        full_view = False

        if output_path is not None:
            if full_view:
                view_path = output_path + "/raw_view_" + str(i)
                mjp.to_csv(view_path, encoding='latin1', index=False)
            view_path = output_path + "/view_" + str(i) + ".csv"
            proj_view.to_csv(view_path, encoding='latin1', index=False)  # always store this

        i += 1
    print("total views:", i)
    return i


def start(vs, ci, attrs, values, types, number_jps=5, output_path=None, full_view=False, interactive=False, offset = 1):
    msg_vspec = """
                ######################################################################
                #                      View Specification                            #
                #          Goal: Reduce the column choice space for users            #                 
                # 1. Cluster identical columns into one group                        #
                # 2. Rank clusters based on the number of examples contained in them #
                ######################################################################
              """
    print(msg_vspec)
    filter_drs = {}
    col_values = {}
    column_idx = 0
    column_clusters = ci.get_clusters(attrs, values, types)
    for column in column_clusters:
        idx = 0
        print(Colors.OKBLUE + "NAME: " + column[0]["name"] + Colors.CEND)
        for cluster in column:
            print(Colors.OKCYAN + "CLUSTER " + str(idx) + Colors.CEND)
            print(Colors.OKGREEN + "SAMPLE_SCORE:" + str(cluster["sample_score"]) + "\t" + "Data Type:" + cluster["type"] + Colors.CEND)
            print(tabulate(cluster["data"], headers=['id', 'Table Name', 'Attribute Name', 'Sample Score', 'Highlight'], tablefmt='fancy_grid'))
            # print(Colors.CBOLD + "\tSamples" + Colors.CEND)
            # for value in cluster["head_values"]:
            #     value = str(value)
            #     if value[0:4] == "<em>":
            #         value = value.replace("<em>", "")
            #         value = value.replace("</em>", "")
            #         print(Colors.WARNING + "\t" + value + Colors.CEND)
            #     else:
            #         print("\t" + str(value))
            print('\n')
            idx += 1
        selected = input(Colors.HEADER + "Enter clusters you want or enter \"all\" instead:" + Colors.CEND)
        sel_idx = []
        if selected == "all":
            sel_idx = range(len(column_clusters[column_idx]))
        else:
            clusters = selected.split(",")
            for cluster in clusters:
                sel_idx.append(int(cluster.strip()))
        hits = vs.clusters2Hits(column, sel_idx)
        filter_drs[(column[0]["name"], FilterType.ATTR, column_idx)] = hits
        col_values[column[0]["name"]] = [row[column_idx] for row in values]
        column_idx += 1
        print("\n")
    msg_vsearch = """
                   ######################################################################
                   #                          View Search                               #
                   #          Goal: Find how to combine relevant tables found in        #
                   #                the view specification                              #
                   #  1. Explore all join paths to combine relevant tables              #
                   #  2. Prune identical join paths                                     #
                   #  3. Rate all join paths                                            #
                   ######################################################################
                 """
    print(msg_vsearch)
    view_metadata_mapping = dict()
    i = 0
    perf_stats = dict()
    st_runtime = time.time()
    for mjp, attrs_project, metadata in vs.search_views(col_values, filter_drs, perf_stats, max_hops=2,
                                                        debug_enumerate_all_jps=False, offset=offset):
        proj_view = dpu.project(mjp, attrs_project)

        if output_path is not None:
            if full_view:
                view_path = output_path + "/raw_view_" + str(i)
                mjp.to_csv(view_path, encoding='latin1', index=False)
            view_path = output_path + "/view_" + str(i)
            proj_view.to_csv(view_path, encoding='latin1', index=False)  # always store this
            # store metadata associated to that view
            view_metadata_mapping[view_path] = metadata

        i += 1

        if interactive:
            print("")
            input("Press any key to continue...")
    et_runtime = time.time()
    perf_stats['runtime'] = (et_runtime - st_runtime)
    pp.pprint(perf_stats)
    if 'num_join_graphs_per_candidate_group' in perf_stats:
        total_join_graphs = sum(perf_stats['num_join_graphs_per_candidate_group'])
        print("Total join graphs: " + str(total_join_graphs))
    if 'materializable_join_graphs' in perf_stats:
        total_materializable_join_graphs = sum(perf_stats['materializable_join_graphs'])
        print("Total materializable join graphs: " + str(total_materializable_join_graphs))

    print("Total views: " + str(i))
    # exit()

    ###
    # Run 4C
    ###
    # return
    # groups_per_column_cardinality = v4c.main(output_path)
    #
    # for k, v in groups_per_column_cardinality.items():
    #     compatible_groups = v['compatible']
    #     contained_groups = v['contained']
    #     complementary_group = v['complementary']
    #     contradictory_group = v['contradictory']
    #
    #     print("Compatible views: " + str(len(compatible_groups)))
    #     print("Contained views: " + str(len(contained_groups)))
    #     print("Complementary views: " + str(len(complementary_group)))
    #     print("Contradictory views: " + str(len(contradictory_group)))



def test_intree(dod):
    for mjp, attrs in test_dpu(dod):
        print(mjp.head(2))


def test_dpu(dod):
    with open("check_debug.pkl", 'rb') as f:
        clean_jp = pickle.load(f)

    for mjp in clean_jp:
        attrs_to_project = dpu.obtain_attributes_to_project(mjp)
        # materialized_virtual_schema = dpu.materialize_join_path(mjp, self)
        materialized_virtual_schema = dpu.materialize_join_graph(mjp, dod)
        yield materialized_virtual_schema, attrs_to_project


def main(args):
    model_path = args.model_path
    separator = args.separator

    store_client = StoreHandler()
    network = fieldnetwork.deserialize_network(model_path)
    dod = ViewSearchPruning(network=network, store_client=store_client, csv_separator=separator)

    attrs = args.list_attributes.split(";")
    values = args.list_values.split(";")
    print(attrs)
    print(values)
    assert len(attrs) == len(values)

    i = 0
    for mjp, attrs_project, metadata in dod.search_views(attrs, values,
                                                         debug_enumerate_all_jps=False):
        print("JP: " + str(i))
        proj_view = dpu.project(mjp, attrs_project)
        print(str(proj_view.head(10)))
        print("Metadata")
        print(metadata)
        if args.output_path:
            if args.full_view:
                mjp.to_csv(args.output_path + "/raw_view_" + str(i), encoding='latin1', index=False)
            proj_view.to_csv(args.output_path + "/view_" + str(i), encoding='latin1', index=False)  # always store this
        i += 1
        if args.interactive == "True":
            print("")
            input("Press any key to continue...")


def pe_paths(dod):
    s = time.time()
    table1 = "Fclt_building_list.csv"
    table2 = "short_course_catalog_subject_offered.csv"
    # table1 = "Warehouse_users.csv"
    # table2 = "short_course_catalog_subject_offered.csv"
    # table1 = "Fclt_building_list.csv"
    # table2 = "Se_person.csv"
    t1 = dod.aurum_api.make_drs(table1)
    t2 = dod.aurum_api.make_drs(table2)
    t1.set_table_mode()
    t2.set_table_mode()
    i = time.time()
    drs = dod.aurum_api.paths(t1, t2, Relation.PKFK, max_hops=2, lean_search=True)
    a = drs.paths()
    e = time.time()
    print("Total time: " + str((e - s)))
    print("Inter time: " + str((i - s)))
    print("Done")


if __name__ == "__main__":
    model_path = config.path_model
    sep = config.separator

    store_client = StoreHandler()
    network = fieldnetwork.deserialize_network(model_path)
    columnInfer = column_infer.ColumnInfer(network=network, store_client=store_client, csv_separator=sep)
    viewSearch = ViewSearchPruning(network=network, store_client=store_client, csv_separator=sep)

    ## MIT DWH

    # tests equivalence and containment - did not finish executing though (out of memory)
    # attrs = ["Mit Id", "Krb Name", "Hr Org Unit Title"]
    # values = ["968548423", "kimball", "Mechanical Engineering"]

    # attrs = ["Subject", "Title", "Publisher"]
    # values = [["", "Man who would be king and other stories", "Oxford university press, incorporated"]]
    # types = ["object", "object", "object"]

    # EVAL - ONE
    # attrs = ["Iap Category Name", "Person Name", "Person Email"]
    # # values = ["", "Meghan Kenney", "mkenney@mit.edu"]
    # values = ["Engineering", "", ""]

    # EVAL - TWO
    # attrs = ["Building Name Long", "Ext Gross Area", "Building Room", "Room Square Footage"]
    # values = ["", "", "", ""]

    # EVAL - THREE
    # attrs = ["Last Name", "Building Name", "Bldg Gross Square Footage", "Department Name"]
    # values = ["Madden", "Ray and Maria Stata Center", "", "Dept of Electrical Engineering & Computer Science"]

    # EVAL - FOUR
    # tests equivalence and containment
    # attrs = ["Email Address", "Department Full Name"]
    # values = ["", ""]

    # EVAL - FIVE
    # attrs = ["Last Name", "Building Name", "Bldg Gross Square Footage", "Department Name"]
    # values = ["", "", "", ""]


    # experiment-1
    # attrs = ["department", ""] # 4,5,6,9,10,11,13,14,15,16,18,19,21 | 2,5,6,7,8,9,10,11,12,15
    # values = [['', 'madden']]
    # types = ["object", "object"]
    # experiment-2: this one stuck
    # attrs = ["", "email"] # 2,5,6,7,8,9,10,11,12,15 | 0,1,2,3
    # values = [['madden', '']]
    # types = ["object", "object"]
    # experiment-3:
    # attrs = ["faculty", "building"] # 2,3,4,5,6,10 | 0,1,2,3
    # values = [['madden', '']]
    # types = ["object", "object"]
    # attrs = ["Building Name Long", "Ext Gross Area", "Building Room", "Room Square Footage"]
    # values = [["", "", "", ""]]
    # types = ["object", "object", "object", "object"]

    # attrs = ["Mit Id", "Krb Name", "Hr Org Unit Title"]
    # values = [["", "", ""]]
    # types = ["int64", "object", "object"]

    ## CHEMBL22

    # ONE (12)
    attrs = ['assay_test_type', 'assay_category', 'journal', 'year', 'volume']
    values = [['', '', '', '', '']]
    types = ["object", "object", "object", "object", "object"]

    # TWO (27)
    # attrs = ['accession', 'sequence', 'organism', 'start_position', 'end_position']
    # values = ['O09028', '', 'Rattus norvegicus', '', '']

    # THREE (50)
    # attrs = ['ref_type', 'ref_url', 'enzyme_name', 'organism']
    # values = ['', '', '', '']

    # FOUR (54)
    # attrs = ['hba', 'hbd', 'parenteral', 'topical']
    # values = ['', '', '', '']

    # FIVE (100-)
    # attrs = ['accession', 'sequence', 'organism', 'start_position', 'end_position']
    # values = ['', '', '', '', '']

    start(viewSearch, columnInfer, attrs, values, types, number_jps=10, output_path=config.output_path)