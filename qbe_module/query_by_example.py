from typing import List
from qbe_module.column_selection import ColumnSelection, Column
from qbe_module.join_path_search import JoinPathSearch
from qbe_module.join_graph_search import JoinGraphSearch
from aurum_api.algebra import AurumAPI

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
    
    def get_column_clusters(self, candidate_list, prune=False):
        column_clusters = []
        for i, candidate in enumerate(candidate_list):
            print("num", i, "column")
            column_cluster = self.column_selection.cluster_columns(candidate, prune)
            column_clusters.append(column_cluster)
        return column_clusters

    def find_join_graphs_between_candidate_columns(self, candidate_list: List[List[Column]], order_chain_only=False):
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
        
        