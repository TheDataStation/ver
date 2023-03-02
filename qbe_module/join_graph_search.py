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
        # attrs needed in the materialization process {tbl -> [attrs needed]}
        attrs_needed = defaultdict(set)
        columns_to_project_dict = defaultdict(list)

        for edge, path in self.graph_dict.items():
            start, end = edge[0], edge[1]
            proj_attrs = path.tbl_proj_attrs
            columns_to_project_dict[start] = proj_attrs[0]
            columns_to_project_dict[end] = proj_attrs[1]

        columns_to_proj = [(k, v) for k, v in columns_to_project_dict.items()]

        columns_to_proj = sorted(columns_to_proj, key=lambda x: x[0])

        columns_to_proj = [columns_to_proj_pair[1] for columns_to_proj_pair in columns_to_proj]

        for path in self.paths:
            for join_pair in path.path:
                if len(join_pair[0].field_name):
                    attrs_needed[join_pair[0].source_name].add(join_pair[0].field_name)
                if len(join_pair[1].field_name):
                    attrs_needed[join_pair[1].source_name].add(join_pair[1].field_name)
            
            for attr_list in path.tbl_proj_attrs:
                for attr in attr_list:
                    attrs_needed[attr.tbl_name].add(attr.attr_name)
         
        return attrs_needed, columns_to_proj
    
    def display(self):
        i = 0
        for edge, path in self.graph_dict.items():
            print(edge)
            print(path.to_str())
            print("attributes to project")
            for i, attrs in enumerate(path.tbl_proj_attrs):
                print("column {} candidates".format(edge[i]))
                print([attr.to_str() for attr in attrs])
        

class JoinGraphSearch:
    def __init__(self, join_path_search: JoinPathSearch):
        self.join_path_search = join_path_search

    def get_join_path_map(self, candidate_lists: List):
        self.col_num = len(candidate_lists)
        join_path_map = {}
        # group candidate columns by their table {tbl -> [columns]}
        cand_dict_list = []
        for cand_list in candidate_lists:
            # cand_list is a list of candidates w.r.t. an example column
            # we reorganize cand_list into a cand_dict {tbl_name -> (columns)}
            cand_dict = defaultdict(list)
            for cand in cand_list:
                cand_dict[cand.tbl_name].append(cand)
            cand_dict_list.append(cand_dict)
        
        for i in range(self.col_num):
            for j in range(i+1, self.col_num):
                src_dict = cand_dict_list[i]
                src_tbls = list(src_dict.keys())
                tgt_dict = cand_dict_list[j]
                tgt_tbls = list(tgt_dict.keys())
                
                all_paths = defaultdict(list)
                
                for src_tbl in src_tbls:
                    paths = self.join_path_search.find_join_paths_between_two_tbls(src_tbl, tgt_tbls, src_dict, tgt_dict)
                    all_paths[src_tbl].extend(paths)
                    
                if len(all_paths) != 0:
                    join_path_map[(i, j)] = all_paths
                   
        return join_path_map

    def find_join_graphs(self, candidate_lists: List):
        join_path_map = self.get_join_path_map(candidate_lists)
        # print(join_path_map)
        edges = list(join_path_map.keys())
        valid_graphs = []
        
        for subset in itertools.combinations(edges, len(edges)-1):
            if self.is_graph_valid(subset, self.col_num):
                valid_graphs.append(subset)

        all_join_graphs = []
        for valid_graph in valid_graphs:
            print(valid_graph)
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