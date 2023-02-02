from typing import List
from ver_utils.join_path_search import JoinPathSearch
import itertools
from collections import defaultdict
from copy import deepcopy

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
            for join_pair in path.path:
                tbl_attrs_join_key_map[join_pair[0].source_name].append(join_pair[0].field_name)
                tbl_attrs_join_key_map[join_pair[1].source_name].append(join_pair[1].field_name)
            proj_map = path.tbl_proj_attrs
            for tbl, attr_list in proj_map.items():
                tbl_attrs_proj_map[tbl].extend(attr_list)
        return tbl_attrs_proj_map, tbl_attrs_join_key_map
        

class JoinGraphSearch:
    def __init__(self, join_path_search: JoinPathSearch):
        self.join_path_search = join_path_search

    """
    a map from candidate column pair to paths between them
    """
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

        # n-1 edges form a tree (n is the number of total edges)
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
                if not self.is_join_graph_valid(join_graph):
                    continue
                join_graph = JoinGraph(join_graph)
                all_join_graphs.append(join_graph)
        return all_join_graphs

    def is_join_graph_valid(self, join_graph):
        joints = defaultdict(list)
        for k, join_path in join_graph.items():
            src, tgt = join_path.path[0][0], join_path.path[-1][-1]
            joints[k[0]].append(src)
            joints[k[1]].append(tgt)
        
        for k, v in joints.items():
            if len(v) > 1:
                v_set = set()
                for _v in v:
                    v_set.add(_v.source_name)

                if len(v_set) > 1:
                    return False

        # check if there is cycle in a join graph
        visited = set()
        # get all join_paths in a join graph
        join_paths = list(join_graph.values())

        for join_path in join_paths:
            for path in join_path.path:
                src, tgt = path[0], path[1]
                if (src, tgt) in visited:
                    return False
                visited.add((src, tgt))
        return True

    def is_graph_valid(self, graph, col_num):
        visited = set()
        for edge in graph:
            visited.add(edge[0])
            visited.add(edge[1])
        return len(visited) == col_num