from typing import List
from qbe_module.join_path_search import JoinPathSearch
import itertools
from collections import defaultdict
from copy import deepcopy
from enum import Enum

class JoinGraphType(Enum):
    JOIN = 1
    NO_JOIN = 2


class CandidateGroup:
    def __init__(self, cand_tbls, project_options):
        # tbls that cover all example columns
        self.cand_tbls = cand_tbls
        # options to project columns in cand_tbls
        self.project_options = project_options


class JoinGraph:
    def __init__(self, cand_group: CandidateGroup, graph_dict):
        self.cand_group = cand_group
        cand_tbls = self.cand_group.cand_tbls
        if len(cand_tbls) == 1:
            # means there is no join needed
            self.tbl = cand_tbls[0]
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
    
    def get_attrs_needed(self, tbl_cols):
        # get attrs needed in the materialization process {tbl -> [attrs needed]}
        attrs_needed = {}
        for tbl in self.cand_group.cand_tbls:
            attrs_needed[tbl] = list(itertools.chain(*list(tbl_cols[tbl].values())))
            attrs_needed[tbl] = set([col.attr_name for col in attrs_needed[tbl]])

        # add join key to attrs needed
        if self.type == JoinGraphType.JOIN:
            for path in self.paths:
                for join_pair in path.path:
                    if len(join_pair[0].field_name):
                        attrs_needed[join_pair[0].source_name].add(join_pair[0].field_name)
                    if len(join_pair[1].field_name):
                        attrs_needed[join_pair[1].source_name].add(join_pair[1].field_name)
            
        # get all projection options
        proj_ops = self.cand_group.project_options
        columns_to_proj_lst = []
        for proj_op in proj_ops:
            proj_map = defaultdict(list)
            for tbl, col_list in proj_op.items():
                for col in col_list:
                    proj_map[col].extend(tbl_cols[tbl][col])
            
            columns_to_proj = [(k, v) for k, v in proj_map.items()]

            columns_to_proj = sorted(columns_to_proj, key=lambda x: x[0])

            columns_to_proj = [columns_to_proj_pair[1] for columns_to_proj_pair in columns_to_proj]

            columns_to_proj_lst.append(columns_to_proj)

        return attrs_needed, columns_to_proj_lst
    
    def to_str(self):
        if self.type is JoinGraphType.NO_JOIN:
            return self.tbl
        else:
            join_paths = []
            for _, path in self.graph_dict.items():
                join_paths.append(path.to_str())
            return "\n".join(join_paths)

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

    def find_join_graphs(self, cand_group: CandidateGroup, chain_only=True):
        cand_tbls = cand_group.cand_tbls
        if len(cand_tbls) == 1:
            return [JoinGraph(cand_group, None)]
        join_path_map = self.get_join_path_map(cand_tbls)
        edges = list(join_path_map.keys())
        graph_skeleton = []
        if not chain_only:
            # we consider all graph topologies
            graph_skeleton = list(itertools.combinations(edges, len(cand_tbls)-1))
        else:
            graph_skeleton.append([(cand_tbls[i], cand_tbls[i+1]) for i in range(len(cand_tbls)-1)])
        join_graph_lst = []
        for graph in graph_skeleton:
            join_graphs = self.dfs_graph(graph, join_path_map)
            for _join_graph in join_graphs:
                join_graph = JoinGraph(cand_group, _join_graph)
                join_graph_lst.append(join_graph)
           
        return join_graph_lst

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