from collections import defaultdict

from aurum_api.algebra import AurumAPI
from archived.DoD import data_processing_utils as dpu

class ViewSpec:
    def __init__(self, network, store_client, csv_separator=","):
        self.aurum_api = AurumAPI(network=network, store_client=store_client)
        self.paths_cache = dict()
        dpu.configure_csv_separator(csv_separator)

    def benchmark_1(self, all_candidates):
        results = {}
        for col, columns in all_candidates.items():
            results[col] = columns
        return results

    def benchmark_2(self, all_candidates, example_hit_dict):
        # select columns with highest containment scores
        results = {}
        for col, candidates in all_candidates:
            candidate_score = list(example_hit_dict[col].items())
            candidate_score.sort(key=lambda x: x[1], reverse=True)
            max_score = candidate_score[0][1]
            result = []
            for x in candidates:
                if example_hit_dict[col][x] != max_score:
                    continue
                result.append(x)
            results[col] = result
        return results

    def cluster_1(self, all_candidates, example_hit_dict):
        # select columns with highest containment scores and its neighbors
        results = {}
        for col, sample_scores in example_hit_dict.items():
            max_score = max(sample_scores.values())
            candidates = all_candidates[col]
            result = set()
            for c in candidates:
                k = (c.source_name, c.field_name)
                if sample_scores[k] != max_score:
                    continue
                result.add(c)
                neighbors = self.aurum_api.content_similar_to(c)
                for neighbor in neighbors:
                    result.add(neighbor)
            results[col] = list(result)
        return results

    def cluster_2(self, all_candidates, example_hit_dict):
        results_hits = {}
        for col, sample_scores in example_hit_dict.items():
            candidates = all_candidates[col]
            visited = defaultdict(bool)
            column_cluster = defaultdict(int)
            idx = 0
            for candidate in candidates:
                if candidate.nid not in visited:
                    visited[candidate.nid] = True
                    cluster = [candidate]
                    target_idx = idx
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
            clusters_hit = defaultdict(list)
            clusters_score = defaultdict(list)
            max_score = 0
            for column, cluster_idx in column_cluster.items():
                k = (column.source_name, column.field_name)
                sample_score = sample_scores[k]
                if sample_score > max_score:
                    max_score = sample_score
                clusters_hit[cluster_idx].append(column)
                clusters_score[cluster_idx].append(sample_score)

            result_hit = []
            for idx, columns in clusters_hit.items():
                if max(clusters_score[idx]) == max_score:
                    result_hit.extend(columns)

            results_hits[col] = result_hit
        return results_hits

