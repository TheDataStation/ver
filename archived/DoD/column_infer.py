from aurum_api.algebra import AurumAPI
from archived.DoD import FilterType
from archived.DoD import data_processing_utils as dpu
from aurum_api.apiutils import DRS, Operation, OP
from collections import defaultdict
import os
import time

class ClusterItem:
    nid = ""
    source_name = ""
    field_name = ""
    tfidf_score = 0
    sample_score = 0
    highlight = []

    def __init__(self, nid, source_name, field_name, tfidf_score, sample_score, highlight):
        self.nid = nid
        self.source_name = source_name
        self.field_name = field_name
        self.tfidf_score = tfidf_score
        self.sample_score = sample_score
        self.highlight = highlight

    def __str__(self):
        return "ClusterItem(nid:%s; source_name:%s; field_name:%s; tfidf_score:%d; sample_score:%d)" % (self.nid, self.source_name, self.field_name, self.tfidf_score, self.sample_score)


class ColumnInfer:

    def __init__(self, network, store_client, csv_separator=","):
        self.aurum_api = AurumAPI(network=network, store_client=store_client)
        dpu.configure_csv_separator(csv_separator)
        self.topk = 300  # magic top k number

    """
        Get candidate columns for one attr
        Returns:
        column_candidates: A dictionary of one attr and its candidate columns
        example_hit_dict: A dictionary of one attr and its sample hit number
        hit_type_dict: A dictionary of one attr and its hit type {CELL, ATTR, CELL_AND_ATTR}
    """
    def infer_candidate_columns(self, attrs, values, topk):
        self.topk = topk
        spread_sheet = []
        for i in range(len(attrs)):
            spread_sheet.append((attrs[i], [row[i] for row in values]))
        print(spread_sheet)
        column_candidates = dict()  # candidate columns for one attr
        column_id = 0
        empty_str = ''

        example_hit_dict = dict()  # dict of column_example_hit. each attribute has a example_hit dict
        hit_type_dict = dict() # dict of hit_type. each attribute has a hit_type dict
        example_match_dict = dict()
        key_hit_dict = dict()

        for item in spread_sheet:
            attr = item[0]
            samples = item[1]
            drs_attr = None
            column_example_hit = defaultdict(int)  # the number of samples contained in one column - column containment score
            hit_type = defaultdict(int)
            example_match = defaultdict(list)

            if attr != "":
                drs_attr = self.aurum_api.search_exact_attribute(attr, max_results=self.topk)
                if len(drs_attr.data) == 0:
                    drs_attr = self.aurum_api.search_attribute(attr, max_results=self.topk)
                for x in drs_attr:
                    column_example_hit[(x.source_name, x.field_name)] = 0
                    hit_type[(x.source_name, x.field_name)] = FilterType.ATTR
            drs_samples_union = None
            if empty_str.join(samples) != "":
                # if user provides samples
                drs_union_candidate = []
                for sample in samples:
                    print(sample)
                    if sample == "":
                        continue
                    drs_sample = self.aurum_api.search_exact_content(sample, max_results=self.topk)
                    # print(drs_sample.data)
                    if len(drs_sample.data) == 0:
                        drs_sample = self.aurum_api.search_content(sample, max_results=self.topk)
                        # print (drs_sample.data)
                    print("number of columns before removing redundant:", len(drs_sample.data))
                    drs_sample_col = self.remove_redundant(drs_sample, sample)
                    for x in drs_sample_col:
                        k = (x.source_name, x.field_name)
                        key_hit_dict[k] = x
                        hit_example = set()
                        max_containment = 0
                        # for h in x.highlight:
                        #     h = h.replace("<em>", "").replace("</em>", "")
                        #     c_score = min(len(sample)/len(h), len(h)/len(sample))
                        #     max_containment = max(max_containment, c_score)
                        #     hit_example.add(h)
                        # if max_containment < 0.5:
                        #     continue
                        # column_example_hit[k] += max_containment
                        column_example_hit[k] += 1
                        example_match[(x.source_name, x.field_name)].extend(list(hit_example))
                        if hit_type[k] == FilterType.ATTR:
                            hit_type[k] = FilterType.ATTR_CELL
                        else:
                            hit_type[k] = FilterType.CELL

                    drs_union_candidate.append(drs_sample)
                drs_samples_union = self.aurum_api.union(drs_union_candidate[0], drs_union_candidate[0])
                for i in range(len(drs_union_candidate)):
                    if i + 1 < len(drs_union_candidate):
                        drs_samples_union = self.aurum_api.union(drs_samples_union, drs_union_candidate[i + 1])

            if attr == "":
                attr = "Column" + str(column_id+1)
            example_hit_dict[attr] = column_example_hit
            hit_type_dict[attr] = hit_type
            example_match_dict[attr] = example_match

            if drs_attr is not None and drs_samples_union is not None:
                # column_candidates[(attr, FilterType.ATTR_CELL, column_id)] = self.get_intersection(drs_attr, drs_samples_union)
                column_candidates[attr] = self.aurum_api.union(drs_attr, drs_samples_union)
            elif drs_attr is None:
                # column_candidates[("Column" + str(column_id+1), FilterType.CELL, column_id)] = drs_samples_union
                column_candidates["Column" + str(column_id + 1)] = drs_samples_union
            else:
                # column_candidates[(attr, FilterType.ATTR, column_id)] = drs_attr
                column_candidates[attr] = drs_attr
            column_id += 1
        return column_candidates, example_hit_dict, hit_type_dict, example_match_dict, key_hit_dict

    def view_spec_benchmark(self, example_hit_dict):
        # select columns with highest containment scores
        print(example_hit_dict)
        final_result = []
        for _, candidates in example_hit_dict.items():
            candidate_score = list(candidates.items())
            candidate_score.sort(key=lambda x: x[1], reverse=True)
            result = []
            max_score = candidate_score[0][1]
            for x in candidate_score:
                if x[1] != max_score:
                    break
                result.append(x[0])
            final_result.append(result)
        print(final_result)
        return final_result

    def view_spec_cluster(self, all_candidates, example_hit_dict):
        # select columns with highest containment scores and its neighbors
        results = []
        results_hits = []
        for col, sample_scores in example_hit_dict.items():
            max_score = max(sample_scores.values())
            candidates = all_candidates[col]
            result = set()
            result_hit = set()
            for c in candidates:
                k = (c.source_name, c.field_name)
                if sample_scores[k] != max_score:
                    continue
                result.add(k)
                result_hit.add(c)
                neighbors = self.aurum_api.content_similar_to(c)
                for neighbor in neighbors:
                    result.add((neighbor.source_name, neighbor.field_name))
                    result_hit.add(neighbor)
            results.append(list(result))
            results_hits.append(result_hit)
        return results, results_hits

    def view_spec_cluster_union_find(self, all_candidates, example_hit_dict, output_path):
        log_path = output_path + "/log_col_sel.txt"
        log = open(log_path, "w", buffering=1)

        results = []
        results_all = []
        results_hits = {}

        print("start")
        start = time.time()
        edge_cnt = 0
        for col, sample_scores in example_hit_dict.items():
            candidates = all_candidates[col]
            log.write(str(col) + "\n")
            log.write("column number before:" + str(len(candidates.data)) + "\n")
            roots = {}
            nid_to_candidate = {}
            cluster = defaultdict(list)
            cluster_score = defaultdict(int)
            for candidate in candidates:
                roots[candidate.nid] = candidate.nid
                nid_to_candidate[candidate.nid] = candidate

            for idx, candidate in enumerate(candidates):
                if idx % 1000 == 0:
                    print("edge cnt:", edge_cnt)
                get_neighbor = time.time()
                neighbors = self.aurum_api.content_similar_to(candidate)
                print("neighor time:", time.time() - get_neighbor)
                # print("neighbor size:", len(neighbors.data))
                for neighbor in neighbors.data:
                    edge_cnt += 1
                    if neighbor.nid not in nid_to_candidate:
                        continue
                    self.merge_root(roots, candidate.nid, neighbor.nid)
            
            print("union find costs", time.time() - start)
            print("begin making clusters")
            global_max_score = 0

            for candidate in candidates:
                k = (candidate.source_name, candidate.field_name)
                score = sample_scores[k]
                cluster_id = self.find_root(roots, candidate.nid)
                cluster[cluster_id].append(candidate)
                cluster_score[cluster_id] = max(cluster_score[cluster_id], score)
                global_max_score = max(global_max_score, score)
            
            result = []
            for cluster_id, score in cluster_score.items():
                if score == global_max_score:
                    result.extend(cluster[cluster_id])
            
            results_hits[col] = result
            log.write("column number after:" + str(len(result)) + "\n")
        return None, None, results_hits
    
    def find_root(self, roots, nid):
        nids = [nid]
        cur = nid
        while roots[cur] != cur:
            cur = roots[cur]
            nids.append(cur)
        for id in nids:
            roots[id] = cur
        return cur
    
    def merge_root(self, roots, nid1, nid2):
        roots[self.find_root(roots, nid1)] = self.find_root(roots, nid2)

    def view_spec_cluster2(self, all_candidates, example_hit_dict, output_path):
        if not os.path.exists(os.path.dirname(output_path)):
            try:
                os.makedirs(os.path.dirname(output_path))
            except OSError as exc:  # Guard against race condition
                if exc.errno != errno.EEXIST:
                    raise
        log_path = output_path + "/log2.txt"
        log = open(log_path, "w", buffering=1)
        results = []
        results_all = []
        results_hits = {}
        print("start")
        no = 0
        for col, sample_scores in example_hit_dict.items():
            candidates = all_candidates[col]
            log.write(str(col) + "\n")
            log.write("column number before:" + str(len(candidates.data)) + "\n")
            no += 1

            visited = defaultdict(bool)
            column_cluster = defaultdict(int)
            idx = 0
            for candidate in candidates:
                if candidate.nid not in visited:
                    visited[candidate.nid] = True
                    cluster = [candidate]
                    target_idx = idx
                    neighbors = self.aurum_api.content_similar_to(candidate)
                    # print("neighbor size:", len(neighbors.data))
                    for neighbor in neighbors.data:
                        if neighbor not in candidates.data:
                            continue
                        if neighbor.nid in visited:
                            # merge into the previous cluster
                            target_idx = column_cluster[neighbor]
                        cluster.append(neighbor)
                        visited[neighbor.nid] = True
                    for x in cluster:
                        column_cluster[x] = target_idx
                    idx += 1
            clusters = defaultdict(list)
            clusters_hit = defaultdict(list)
            clusters_score = defaultdict(list)
            max_score = 0
            for column, cluster_idx in column_cluster.items():
                k = (column.source_name, column.field_name)
                sample_score = sample_scores[k]
                if sample_score > max_score:
                    max_score = sample_score
                clusters[cluster_idx].append(k)
                clusters_hit[cluster_idx].append(column)
                clusters_score[cluster_idx].append(sample_score)
           
            log.write("number of clusters:" + str(len(clusters)) + "\n")
            log.write("max score:" + str(max_score) + "\n")
            final_cluster = []
            final_cluster_all = []
            result_hit = []

            selected = 0
            # for idx, columns in clusters.items():
            #     final_cluster_all.append(columns)
            #     if max(clusters_score[idx]) == max_score:
            #         final_cluster.extend(columns)

            for idx, columns in clusters_hit.items():
                if max(clusters_score[idx]) == max_score:
                    selected += 1
                    result_hit.extend(columns)
            log.write("number of clusters selected:" + str(selected) + "\n")
            log.write("number of columns after:" + str(len(result_hit)) + "\n")

            results.append(final_cluster)
            # results_all.append(final_cluster_all)
            results_hits[col] = result_hit
        return results, results_all, results_hits


    def view_spec_cluster_exploration(self, all_candidates, example_hit_dict):
        # select columns with highest containment scores and its neighbors
        results = []
        for col, sample_scores in example_hit_dict.items():
            max_score = max(sample_scores.values())
            candidates = all_candidates[col]
            result = set()
            max_size = 0
            max_df = None
            max_k = None
            for c in candidates:
                k = (c.source_name, c.field_name)
                if sample_scores[k] != max_score:
                    continue
                result.add(k)
                df = dpu.read_column("/Users/gongyue/data/opendata/"+k[0], k[1])
                if len(df) > max_size:
                    max_size = len(df)
                    max_df = df
                    max_k = k
                neighbors = self.aurum_api.content_similar_to(c)
                for neighbor in neighbors:
                    result.add((neighbor.source_name, neighbor.field_name))
                # sample from max_df
                samples = max_df[max_k[1]].sample(n=5).tolist()
                column_example_hit = defaultdict(int)
                for sample in samples:
                    if sample == "":
                        continue
                    drs_sample = self.aurum_api.search_exact_content(sample, max_results=self.topk*3)
                    drs_sample_col = self.remove_redundant(drs_sample, sample)
                    for x in drs_sample_col:
                        k = (x.source_name, x.field_name)
                        hit_example = set()
                        max_containment = 0
                        for h in x.highlight:
                            h = h.replace("<em>", "").replace("</em>", "")
                            c_score = len(sample)/len(h)
                            max_containment = max(max_containment, c_score)
                            hit_example.add(h)
                        column_example_hit[k] += max_containment
                for k, v in column_example_hit.items():
                    if v >= 2:
                        result.add(k)
            results.append(list(result))
        return results

    def get_purity(self, results, gt_path):
        df = dpu.read_relation_on_copy(gt_path)
        groundTruth = []
        for _, row in df.iterrows():
            groundTruth.append((row['candidate_table'].replace("____", "_"), row['candidate_col_name']))
        for result in results:
            for cluster in result:
                TP, FP, FN = 0, 0, 0
                for x in cluster:
                    if x in groundTruth:
                        TP += 1
                    else:
                        FP += 1
                score = TP/len(cluster)
                print(score)

    def get_statistics(self, results, gt_path):
        df = dpu.read_relation_on_copy(gt_path)
        groundTruth = []
        for _, row in df.iterrows():
            groundTruth.append((row['candidate_table'].replace("____", "_"), row['candidate_col_name']))
        stats = []
        for result in results:
            TP, FP, FN = 0, 0, 0
            for x in result:
                if x in groundTruth:
                    TP += 1
                else:
                    FP += 1
            for x in groundTruth:
                if x not in result:
                    FN += 1
            Precision = TP/(TP+FP)
            Recall = TP/(TP+FN)
            F1 = 2*Precision*Recall/(Precision + Recall)
            stats.append([Precision, Recall, F1])
        return stats

    def print_stats(self, stats, unit):
        for (idx, stat) in enumerate(stats):
            precision = stat[0]
            recall = stat[1]
            f1 = stat[2]

            if idx < unit:
                prefix = "low"
            elif idx < unit * 2:
                prefix = "mid"
            else:
                prefix = "high"
            print(prefix, round(precision, 3), '\t', round(recall, 3), '\t', round(f1, 3))

    def cluster_based_on_hit_type(self, example_match_dict):
        clusters = defaultdict(list)
        for hit, hit_type in example_match_dict.items():
            clusters[tuple(hit_type)].append(ClusterItem(hit[0], hit[1], hit[2], 0, len(hit_type), hit_type))
        return clusters.values()

    """
        Cluster Identical Columns 
        (could be potentially accelerated through hashvalue metadata and column store)
        Returns:
        clusters: A list of ClusterItem
    """
    def cluster_columns(self, candidates, score_map, example_match, hit_type):
        visited = defaultdict(bool)
        clusters = []
        column_cluster = defaultdict(int)
        idx = 0
        for candidate in candidates:
            if candidate.nid not in visited:
                # print(candidate.source_name, candidate.field_name)
                visited[candidate.nid] = True
                cluster = [candidate]
                target_idx = idx
                print(candidate)
                neighbors = self.aurum_api.content_similar_to(candidate)
                for neighbor in neighbors.data:
                    if neighbor.nid in visited:
                        # merge into the previous cluster
                        target_idx = column_cluster[neighbor]
                    cluster.append(neighbor)
                    visited[neighbor.nid] = True
                for x in cluster:
                    column_cluster[x] = target_idx
                idx += 1
        clusters = defaultdict(list)
        for column, cluster_idx in column_cluster.items():
            k = (column.source_name, column.field_name)
            sample_score = score_map[k]
            if sample_score > 0 or hit_type[k] == FilterType.ATTR:
                clusters[cluster_idx].append(ClusterItem(column.nid, column.source_name, column.field_name, 0, sample_score, example_match[k]))
        return clusters.values()

    def get_clusters(self, attrs, values, types):
        candidate_columns, sample_score, hit_type_dict, match_dict, _ = self.infer_candidate_columns(attrs, values)
        attr_clusters = []
        idx = 0
        for column, candidates in candidate_columns.items():
            clusters = self.cluster_columns(candidates.data, sample_score[column], match_dict[column], hit_type_dict[column])
            # clusters = self.cluster_based_on_hit_type(match_dict[column[0]])
            clusters_list = []
            for cluster in clusters:
                tmp = dict()
                # head_values, data_type = self.get_head_values_and_type(cluster[0], 5)
                # if len(head_values) == 0 or data_type.name != types[idx]:
                #     continue       # discard empty columns
                # if len(head_values) == 0:
                #     continue
                tmp["name"] = column
                # tmp["sample_score"], max_column = self.get_containment_score(cluster)
                tmp["sample_score"] = 0
                # tmp["data"] = list(map(lambda x: (x.nid, x.source_name, x.field_name, x.tfidf_score), cluster))
                tmp["data"] = list(map(lambda x: (x.nid, x.source_name, x.field_name, x.sample_score, x.highlight), cluster))
                # tmp["type"] = data_type.name
                tmp["type"] = "object"
                # tmp["head_values"] = list(set(max_column.highlight)) + head_values
                # tmp["head_values"] = cluster[0].highlight
                clusters_list.append(tmp)
            sorted_list = sorted(clusters_list, key=lambda e: e.__getitem__('sample_score'), reverse=True)
            attr_clusters.append(sorted_list)
            idx += 1
        return attr_clusters

    def evaluation(self, attrs, values, gt_path):
        candidate_columns, sample_score, hit_type_dict, match_dict = self.infer_candidate_columns(attrs, values)
        results = []
        for _, columns in candidate_columns.items():
            results.append([(c.source_name, c.field_name) for c in columns])

        print("method1")
        stats_truth = self.get_statistics(results, gt_path)
        self.print_stats(stats_truth, len(values[0]) / 3)

        print("method2")
        results = self.view_spec_benchmark(sample_score)
        stats = self.get_statistics(results, gt_path)
        self.print_stats(stats, len(values[0]) / 3)

        print("method3")
        results = self.view_spec_cluster(candidate_columns, sample_score)
        stats = self.get_statistics(results, gt_path)
        self.print_stats(stats, len(values[0]) / 3)

        print("method4")
        results, results_all = self.view_spec_cluster2(candidate_columns, sample_score)
        tats = self.get_statistics(results, gt_path)
        self.print_stats(stats, len(values[0]) / 3)

    def view_search_evaluation(self, attrs, values):
        candidate_columns, sample_score, hit_type_dict, match_dict, hit_dict = self.infer_candidate_columns(attrs, values)
        results = []
        for _, columns in candidate_columns.items():
            results.append([(c.source_name, c.field_name) for c in columns])

        num1 = self.get_candidate_groups_num(results)

        results = self.c(sample_score)
        num2 = self.get_candidate_groups_num(results)

        results = self.view_spec_cluster(candidate_columns, sample_score)
        num3 = self.get_candidate_groups_num(results)

        results, results_all = self.view_spec_cluster2(candidate_columns, sample_score)
        num4 = self.get_candidate_groups_num(results)

        print(num1, num2, num3, num4)

    def get_candidate_groups_num(self, results):
        candidate_groups_num = 1
        prv_result = None
        num1 = len(set(results[0]).difference(results[1]))
        num2 = len(set(results[1]).difference(results[0]))
        # results.sort(key=len)
        # for result in results:
        #     if prv_result is None:
        #         candidate_groups_num *= len(result)
        #     else:
        #         candidate_groups_num *= max(len(set(result).difference(prv_result)), len(prv_result.difference(set(result))))
        #     prv_result = set(result)
        return num1*num2

    def get_head_values_and_type(self, clusterItem, offset):
        path = self.get_path(clusterItem.nid, clusterItem.source_name)
        df = dpu.read_column(path, clusterItem.field_name, offset)
        top_df = df.dropna().drop_duplicates().values
        return [row[0] for row in top_df], df.dtypes[clusterItem.field_name]

    def get_path(self, nid, table):
        return self.aurum_api.helper.get_path_nid(nid) + table

    @staticmethod
    def get_containment_score(cluster):
        # columns in one cluster should have the same sample containment score
        max_score = -1
        max_column = None
        for column in cluster:
            if column.sample_score > max_score:
                max_score = column.sample_score
                max_column = column
        return max_score, max_column

    @staticmethod
    def correct_cluster_tfidf_score(cluster):
        col_score = {}
        for column in cluster:
            if column.field_name in col_score.keys() and col_score[column.field_name] >= column.tfidf_score:
                continue
            col_score[column.field_name] = column.tfidf_score
        for column in cluster:
            column.tfidf_score = col_score[column.field_name]
        return cluster

    @staticmethod
    def remove_redundant(drs_sample, kw):
        lookup = {}
        for drs in drs_sample:
            lookup[drs.nid] = drs
        return DRS(list(lookup.values()), Operation(OP.KW_LOOKUP, params=[kw]))

    @staticmethod
    def get_tfidf_score(candidate, hit_type):
        if hit_type == FilterType.CELL:
            return 0
        else:
            return round(candidate.score,2)
    
    @staticmethod
    def get_intersection(drs_attr: DRS, drs_sample: DRS) -> DRS:
        lookup = {}
        intersection = []
        if len(drs_attr.data) > len(drs_sample.data):
            lookupDrs = drs_attr
            queryDrs = drs_sample
        else:
            lookupDrs = drs_sample
            queryDrs = drs_attr
        for (idx, hit) in enumerate(lookupDrs.data):
            lookup[(hit.source_name, hit.field_name)] = idx
        for hit in queryDrs.data:
            if (hit.source_name, hit.field_name) in lookup:
                if lookupDrs == drs_sample:
                    intersection.append(lookupDrs.data[lookup[(hit.source_name, hit.field_name)]])
                else:
                    intersection.append(hit)
        return DRS(intersection, Operation(OP.KW_LOOKUP, params=[""]))