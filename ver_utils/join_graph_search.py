from typing import List
from join_path_search import JoinPathSearch
import itertools
from collections import defaultdict

class JoinGraph:
    def __init__(self, graph_dict):
        adj_list = defaultdict(list)
        for edge in graph_dict.keys():
            adj_list[edge[0]].append(edge[1])
            adj_list[edge[1]].append(edge[0])
        self.graph = adj_list
        self.paths = list(graph_dict.values())
        self.graph_dict = graph_dict
    
    def get_attrs_needed(self):
        tbl_attrs_proj_map = defaultdict(list)
        tbl_attrs_join_key_map = defaultdict(list)
        for path in self.paths:
            for join_pair in path:
                tbl_attrs_join_key_map[join_pair[0].source_name].append(join_pair[0].field_name)
                tbl_attrs_join_key_map[join_pair[1].source_name].append(join_pair[1].field_name)
            proj_map = path.attrs_to_project
            for tbl, attr_list in proj_map.items():
                tbl_attrs_proj_map[tbl].extend(attr_list)
        return tbl_attrs_proj_map, tbl_attrs_join_key_map
        

class JoinGraphSearch:
    def __init__(self, join_path_search: JoinPathSearch):
        self.join_path_search = join_path_search

    def get_join_path_map(self, candidate_lists: List):
        self.col_num = len(candidate_lists)
        join_path_map = {}
        for i in range(self.col_num):
            for j in range(i+1, self.col_num):
                src_columns = candidate_lists[i]
                tgt_columns = candidate_lists[j]
                all_paths = []
                for src_column in src_columns:
                    paths = self.join_path_search.find_join_paths_between_two_cols(src_column, tgt_columns)
                    all_paths.extend(paths)
                if len(all_paths) != 0:
                    join_path_map[(i, j)] = all_paths
        return join_path_map

    def find_join_graphs(self, candidate_lists: List):
        join_path_map = self.get_join_path_map(candidate_lists)
        edges = list(join_path_map.keys())
        valid_graphs = []
        for subset in itertools.combinations(edges, len(edges)-1):
            if self.is_graph_valid(subset, self.col_num):
                valid_graphs.append(subset)

        all_join_graphs = []
        for valid_graph in valid_graphs:
            new_dict = {}
            for edge in valid_graph:
               new_dict[edge] = join_path_map[edge]
            keys, values = zip(*new_dict.items())
            join_graphs = [dict(zip(keys, v)) for v in itertools.product(*values)]
            for join_graph in join_graphs:
                join_graph = JoinGraph(join_graph)
                all_join_graphs.append(join_graph)

    def is_graph_valid(self, graph, col_num):
        visited = set()
        for edge in graph:
            visited.add(edge[0])
            visited.add(edge[1])
        return len(visited) == col_num