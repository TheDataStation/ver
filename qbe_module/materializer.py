from qbe_module.join_path_search import JoinPath
from qbe_module.join_graph_search import JoinGraph, JoinGraphType
from qbe_module.io_utils import read_csv_columns_with_sampling
import pandas as pd
from collections import deque, defaultdict
import itertools

class Materializer:
    def __init__(self, table_path: str, tbl_cols, sample_size: int=200, sep: str=','):
        self.sample_size = sample_size
        self.table_path = table_path
        self.tbl_cols = tbl_cols
        self.sep = sep

    def materialize_join_graph(self, join_graph: JoinGraph):
        attrs_needed_map, columns_to_project_lst = join_graph.get_attrs_needed(self.tbl_cols)

        # when a join graph has only a single tbl and no join is needed
        if join_graph.type == JoinGraphType.NO_JOIN:
            tbl = join_graph.tbl
            df = read_csv_columns_with_sampling(self.table_path + tbl, tbl, list(attrs_needed_map[tbl]), self.sample_size, self.sep)
        else:
            graph = join_graph.graph
            graph_dict = join_graph.graph_dict
            start = list(join_graph.graph_dict.keys())[0][0]
            last = start
            node_to_df = {}
            
            # bfs the graph and materialize
            q = deque()
            q.append(start)
            visited = set()
            visited_tbl = set()
            visited.add(start)
            while q:
                cur = q.popleft()
                for nei in graph[cur]:
                    if nei in visited:
                        continue
                    q.append(nei)
                    visited.add(nei)
                    last = nei
                    edge = (cur, nei)
                    join_path = graph_dict[edge]
                    if cur in node_to_df:
                        init_df = node_to_df[cur]
                        df = self.materialize_join_path(join_path, init_df, attrs_needed_map, visited_tbl)
                    else:
                        df = self.materialize_join_path(join_path, None, attrs_needed_map, visited_tbl)
                    node_to_df[cur] = df
                    node_to_df[nei] = df
            
            df = node_to_df[last]
        if len(df) == 0:
            return []
        for columns_to_project in columns_to_project_lst:
            final_attrs_project = list(itertools.product(*columns_to_project))
            
            df_list = []
            for attrs_list in final_attrs_project:
                df_list.append(df[[attr.to_str() for attr in attrs_list]])

        return df_list

    def get_col_name(self, col):
        return "{}.{}".format(col.source_name, col.field_name)

    def materialize_join_path(self, join_path: JoinPath, init_df, attr_needed_map, visited_tbl):
        path = join_path.path
        
        prv_df = init_df
        for join_pair in path:
            key1, key2 = join_pair[0], join_pair[1]
            tbl1, tbl2 = key1.source_name, key2.source_name

            if key1 == key2:
                if prv_df is not None:
                    return prv_df
                else:
                    attrs_needs = attr_needed_map[tbl1]
                    visited_tbl.add(tbl1)
                    return read_csv_columns_with_sampling(self.table_path + tbl1, tbl1, list(attrs_needs), self.sample_size, self.sep)
            
            if prv_df is None:
                attrs_needs1 = attr_needed_map[tbl1]
                df1 = read_csv_columns_with_sampling(self.table_path + tbl1, tbl1, list(attrs_needs1), self.sample_size, self.sep)
                attrs_needs2 = attr_needed_map[tbl2]
                df2 = read_csv_columns_with_sampling(self.table_path + tbl2, tbl2, list(attrs_needs2), self.sample_size, self.sep)
                prv_df = pd.merge(df1, df2, left_on=self.get_col_name(key1), right_on=self.get_col_name(key2), how='inner')
                visited_tbl.add(tbl1)
                visited_tbl.add(tbl2)
            else:
                if len(prv_df) == 0:
                    return prv_df
                
                if tbl2 in visited_tbl:
                    continue
                attrs_needs2 = attr_needed_map[tbl2]
                df = read_csv_columns_with_sampling(self.table_path + tbl2, tbl2, list(attrs_needs2), self.sample_size, self.sep)
                prv_df = pd.merge(prv_df, df, left_on=self.get_col_name(key1), right_on=self.get_col_name(key2), how='inner')
                visited_tbl.add(tbl2)

        return prv_df