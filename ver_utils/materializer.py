from ver_utils.join_path_search import JoinPath
from ver_utils.join_graph_search import JoinGraph
from ver_utils.io_utils import read_csv_columns_with_sampling
import pandas as pd
from collections import deque, defaultdict

class Materializer:
    def __init__(self, table_path: str, sample_size: int=200):
        self.sample_size = sample_size
        self.table_path = table_path

    def materialize_join_graph(self, join_graph: JoinGraph):
        graph = join_graph.graph
        graph_dict = join_graph.graph_dict
        proj_map, join_key_map = join_graph.get_attrs_needed()
        
        start = 0
        last = start
        node_to_df = {}
        
        # bfs the graph and materialize
        q = deque()
        q.append(start)
        visited = set()
        visited.add(start)
        while q:
            cur = q.popleft()
            for nei in graph[cur]:
                if nei in visited:
                    continue
                q.append(nei)
                last = nei
                edge = (cur, nei)
                join_path = graph_dict[edge]
                if start in node_to_df:
                    init_df = node_to_df[start]
                    df = self.materialize_join_path(join_path, init_df, proj_map, join_key_map)
                else:
                    df = self.materialize_join_path(join_path, None, proj_map, join_key_map)
                node_to_df[start] = df
                node_to_df[nei] = df
        
        df = node_to_df[last]
        return df[proj_map.values()]

    def materialize_join_path(self, join_path: JoinPath, init_df=None, tbl_attrs_proj_map=None, tbl_attrs_join_key_map=None):
        path = join_path.path
        if tbl_attrs_proj_map is None:
            tbl_attrs_proj_map = join_path.tbl_proj_attrs
        if tbl_attrs_join_key_map is None:
            tbl_attrs_join_key_map = defaultdict(list)
            for join_pair in path:
                tbl_attrs_join_key_map[join_pair[0].source_name].append(join_pair[0].field_name)
                tbl_attrs_join_key_map[join_pair[1].source_name].append(join_pair[1].field_name)
        
        prv_df = init_df
        for join_pair in path:
            key1, key2 = join_pair[0], join_pair[1]
            tbl1, tbl2 = key1.source_name, key2.source_name

            if key1 == key2:
                attrs_needs = tbl_attrs_proj_map[tbl1]
                return read_csv_columns_with_sampling(self.table_path + tbl1, list(attrs_needs), self.sample_size)
           
            if prv_df is None:
                attrs_needs1 = set(tbl_attrs_join_key_map[tbl1]).union(tbl_attrs_proj_map[tbl1])
                df1 = read_csv_columns_with_sampling(self.table_path + tbl1, list(attrs_needs1), self.sample_size)
                attrs_needs2 = set(tbl_attrs_join_key_map[tbl2]).union(tbl_attrs_proj_map[tbl2])
                df2 = read_csv_columns_with_sampling(self.table_path + tbl2, list(attrs_needs2), self.sample_size)
                prv_df = pd.merge(df1, df2, left_on=key1.field_name, right_on=key2.field_name, how='inner', suffixes=('_x', ''))
            else:
                if len(prv_df) == 0:
                    break
                attrs_needs2 = set(tbl_attrs_join_key_map[tbl2]).union(tbl_attrs_proj_map[tbl2])
                df = read_csv_columns_with_sampling(self.table_path + tbl2, list(attrs_needs2), self.sample_size)
                prv_df = pd.merge(prv_df, df, left_on=key1.field_name, right_on=key2.field_name, how='inner', suffixes=('_x', ''))
        
        if len(prv_df) == 0:
            return prv_df
        src, tgt = path[0][0].source_name, path[-1][1].source_name
        final_proj_attrs = set(tbl_attrs_proj_map[src]).union(set(tbl_attrs_proj_map[tgt]))
        return prv_df[final_proj_attrs]