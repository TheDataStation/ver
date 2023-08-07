from typing import List
from qbe_module.join_path_search import JoinPathSearch
import itertools
from collections import defaultdict
from copy import deepcopy
from enum import Enum

class JoinGraphType(Enum):
    JOIN = 1
    NO_JOIN = 2
  
class JoinGraph:
    def __init__(self, graph_dict, single_tbl: str=None):
        if graph_dict is None:
            # means there is no join needed
            self.tbl = single_tbl
            self.type = JoinGraphType.NO_JOIN
        else:
            self.type = JoinGraphType.JOIN
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
        if self.type is JoinGraphType.NO_JOIN:
            print(self.tbl)
        else:
            i = 0
            for edge, path in self.graph_dict.items():
                # print(edge)
                print(path.to_str())
                # print("attributes to project")
                # for i, attrs in enumerate(path.tbl_proj_attrs):
                #     print("column {} candidates".format(edge[i]))
                #     print([attr.to_str() for attr in attrs])
            

class JoinGraphSearch:
    def __init__(self, join_path_search: JoinPathSearch):
        self.join_path_search = join_path_search
        self.all_paths = {}

    def get_join_path_map(self, cand_group: List):
        self.cur_paths = {}
        tbl_num = len(cand_group)
        for i in range(tbl_num):
            for j in range(i+1, tbl_num):
                src, tgt = cand_group[i], cand_group[j]
                if (src, tgt) not in self.all_paths and (tgt, src) not in self.all_paths:
                    paths = self.join_path_search.find_join_paths(src, tgt)
                    reverse_paths = [x.reverse() for x in paths]
                    self.all_paths[(src, tgt)] = paths
                    self.all_paths[(tgt, src)] = reverse_paths
                self.cur_paths[(src, tgt)], self.all_paths[(tgt, src)] = self.all_paths[(src, tgt)], self.all_paths[(tgt, src)]
        return self.cur_paths


    def get_join_path_map2(self, candidate_lists: List):
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
                print(i, j)
                src_dict = cand_dict_list[i]
                src_tbls = list(src_dict.keys())
                tgt_dict = cand_dict_list[j]
                tgt_tbls = list(tgt_dict.keys())
                
                all_paths = defaultdict(list)
                
                for src_tbl in src_tbls:
                    paths = self.join_path_search.find_join_paths_between_two_tbls(src_tbl, tgt_tbls, src_dict, tgt_dict)
                    all_paths[src_tbl].extend(paths)
                    # print(src_tbl)
                    # for path in paths:
                    #     print(path.to_str())
                    
                if len(all_paths) != 0:
                    join_path_map[(i, j)] = all_paths
        # print(join_path_map)          
        return join_path_map

    def find_join_graphs(self, cand_group: List, chain_only=True):
        if len(cand_group) == 1:
            return [JoinGraph(None, cand_group[0])]
        join_path_map = self.get_join_path_map(cand_group)
        edges = list(join_path_map.keys())
        graph_skeleton = []
        if not chain_only:
            # we consider all graph topologies
            graph_skeleton = list(itertools.combinations(edges, len(cand_group)-1))
        else:
            graph_skeleton.append([(cand_group[i], cand_group[i+1]) for i in range(len(cand_group)-1)])
        join_graph_lst = []
        for graph in graph_skeleton:
            join_graphs = self.dfs_graph(graph, join_path_map)
            for join_graph in join_graphs:
                join_graph = JoinGraph(join_graph)
                join_graph_lst.append(join_graph)
           
        return join_graph_lst

    def find_join_graphs2(self, candidate_lists: List, order_chain_only=False):
        join_path_map = self.get_join_path_map(candidate_lists)
        edges = list(join_path_map.keys())
        valid_graphs = []
        
        if not order_chain_only:
            for subset in itertools.combinations(edges, len(candidate_lists)-1):
                if self.is_graph_valid(subset, self.col_num):
                    valid_graphs.append(subset)
        else:
            valid_graphs.append([(i, i+1) for i in range(len(candidate_lists)-1)])

        all_join_graphs = []
        for valid_graph in valid_graphs:
            # print(valid_graph)
            new_dict = {}
            for edge in valid_graph:
                new_dict[edge] = join_path_map[edge]
                cnt = 0
                for k, v in new_dict[edge].items():
                    # print("src_tbl", k)
                    # for path in v:
                    #     print(path.to_str())
                    cnt += len(v)
                print(edge, cnt)
                new_dict[(edge[1], edge[0])] = join_path_map[edge]
            # print("begin dfs")
            join_graphs = self.dfs_graph(valid_graph, new_dict)
            for join_graph in join_graphs:
                join_graph = JoinGraph(join_graph)
                all_join_graphs.append(join_graph)
           
        return all_join_graphs

    def dfs(self, cur_graph, res, adj_list, edges_dict, path_order, idx):
        if idx == len(path_order):
            res.append(deepcopy(cur_graph))
            # if len(res) % 10000 == 0:
            #     print("num join paths:", len(res))
            return

        cur_edge = path_order[idx]
        start_tbl, edge_list = cur_edge[0], edges_dict[cur_edge]
       
        for edge in edge_list:
            cur_graph[cur_edge] = edge
            self.dfs(cur_graph, res, adj_list, edges_dict, path_order, idx+1)
            cur_graph.pop(cur_edge)
            
    
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

        self.dfs({}, res, adj_list, edges_dict, path_order, 0)
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