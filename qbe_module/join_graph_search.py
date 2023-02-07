from typing import List
from qbe_module.join_path_search import JoinPathSearch
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
        tbl_attrs_proj_map = defaultdict(set)
        tbl_attrs_join_key_map = defaultdict(list)
        for path in self.paths:
            for join_pair in path.path:
                tbl_attrs_join_key_map[join_pair[0].source_name].append(join_pair[0].field_name)
                tbl_attrs_join_key_map[join_pair[1].source_name].append(join_pair[1].field_name)
            proj_map = path.tbl_proj_attrs
            for tbl, attr_list in proj_map.items():
                for attr in attr_list:
                    tbl_attrs_proj_map[tbl].add(attr)
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
                # all_paths = []
                all_paths = defaultdict(list)
                
                for src_column in src_columns:
                    paths = self.join_path_search.find_join_paths_between_two_cols(src_column, tgt_columns)
                    # all_paths.extend(paths)
                    all_paths[src_column.tbl_name].extend(paths)
                    
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
               new_dict[(edge[1], edge[0])] = join_path_map[edge]
           
            join_graphs = self.dfs_graph(valid_graph, new_dict)
            for join_graph in join_graphs:
                join_graph = JoinGraph(join_graph)
                all_join_graphs.append(join_graph)
           
        return all_join_graphs

    def dfs(self, cur_graph, visited, res, adj_list, edges_dict, path_order, idx):
        if idx == len(path_order):
            res.append(deepcopy(cur_graph))
            return

        cur_edge = path_order[idx]
        for start_tbl, edge_list in edges_dict[cur_edge].items():
            if (cur_edge[0] in visited) and (visited[cur_edge[0]] != start_tbl):
                continue
            for edge in edge_list:
                end_tbl = edge.path[-1][-1].source_name
                cur_graph[cur_edge] = edge
                visited[cur_edge[1]] = end_tbl
                self.dfs(cur_graph, visited, res, adj_list, edges_dict, path_order, idx+1)
                cur_graph.pop(cur_edge)
                visited.pop(cur_edge[1])

    
    def dfs_graph(self, graph, edges_dict):
        adj_list = defaultdict(list)
        deg_list = defaultdict(int)
        for edge in graph:
            adj_list[edge[0]].append(edge[1])
            deg_list[edge[0]] += 1
            adj_list[edge[1]].append(edge[0])
            deg_list[edge[1]] += 1

        start = -1
        for node, deg in deg_list.items():
            if deg == 1:
                start = node
                break

        path_order = []

        self.dfs_graph_structure(adj_list, {start}, path_order, start)

        res = []

        self.dfs({}, {}, res, adj_list, edges_dict, path_order, 0)
        return res

    def dfs_graph_structure(self, adj_list, visited, path_order, start):
        for nei in adj_list[start]:
            if not nei in visited:
                next_edge = (start, nei)
                path_order.append(next_edge)
                visited.add(nei)
                self.dfs_graph_structure(adj_list, visited, path_order, nei)

    def is_graph_valid(self, graph, col_num):
        visited = set()
        for edge in graph:
            visited.add(edge[0])
            visited.add(edge[1])
        return len(visited) == col_num