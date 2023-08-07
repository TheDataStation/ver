from typing import List
from qbe_module.column_selection import ColumnSelection, Column
from qbe_module.join_path_search import JoinPathSearch
from qbe_module.join_graph_search import JoinGraphSearch
from aurum_api.algebra import AurumAPI
from collections import defaultdict
from itertools import product

class ExampleColumn:
    def __init__(self, attr: str, examples: List[str]) -> None:
        self.attr = attr
        self.examples = examples
       
class QueryByExample:
    def __init__(self, aurum_api: AurumAPI):
        self.column_selection = ColumnSelection(aurum_api)
        self.join_path_search = JoinPathSearch(aurum_api)
        self.join_graph_search = JoinGraphSearch(self.join_path_search)

    
    def find_candidate_columns(self, columns: List[ExampleColumn], cluster_prune=False):
        self.columns = columns
        candidates_list = []
        for column in self.columns:
            candidate_columns = self.column_selection.column_retreival(column.attr, column.examples)
            candidates_list.append(list(candidate_columns.values()))
        if cluster_prune:
            candidates_list = self.get_column_clusters(candidates_list, prune=True)
        return candidates_list
    
    def find_candidate_groups(self, candidate_list: List[List[Column]]):
        candidate_tbls = [set() for _ in candidate_list] 
        tbl_cols = {}
        for i, candidates in enumerate(candidate_list):
            for col in candidates:
                candidate_tbls[i].add(col.tbl_name)
                if col.tbl_name not in tbl_cols:
                    tbl_cols[col.tbl_name] = defaultdict(list)
                tbl_cols[col.tbl_name][i].append(col)
        # obtain combinations of candidate tbls
        combs = list(product(*candidate_tbls))
        candidate_groups = set()
        for comb in combs:
            candidate_groups.add(tuple(set(comb)))
        return [list(x) for x in candidate_groups], tbl_cols

    def find_join_graphs_for_cand_group(self, cand_group):
        return self.join_graph_search.find_join_graphs(cand_group)

    def find_join_graphs_for_cand_groups(self, cand_groups):
        all_join_graphs = []
        for cand_group in cand_groups:
            all_join_graphs.extend(self.join_graph_search.find_join_graphs(cand_group))
        return all_join_graphs

    def get_column_clusters(self, candidate_list, prune=False):
        column_clusters = []
        for i, candidate in enumerate(candidate_list):
            print("num", i, "column")
            column_cluster = self.column_selection.cluster_columns(candidate, prune)
            column_clusters.append(column_cluster)
        return column_clusters

    def find_join_graphs_between_candidate_columns(self, candidate_list: List[List[Column]], order_chain_only=True):
        return self.join_graph_search.find_join_graphs(candidate_list, order_chain_only)

    def find_joins_between_candidate_columns(self, candidate_list: List[List[Column]]):
        src_cols = candidate_list[0]
        results = []
        for src in src_cols:
            # print(src)
            result = self.join_path_search.find_join_paths_between_two_cols(src, candidate_list[1])
            # print(result)
            results.extend(result)
        return results
        
        