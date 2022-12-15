from ver_utils.join_path_search import JoinPath
from ver_utils.io_utils import read_csv_columns_with_sampling
import pandas as pd
from collections import defaultdict

class Materializer:
    def __init__(self, table_path: str, sample_size: int=200):
        self.sample_size = sample_size
        self.table_path = table_path

    def materialize(self, join_path: JoinPath):
        path = join_path.path
        tbl_attrs_proj_map = join_path.tbl_proj_attrs
        tbl_attrs_join_key_map = defaultdict(list)
        for join_pair in path:
            tbl_attrs_join_key_map[join_pair[0].source_name].append(join_pair[0].field_name)
            tbl_attrs_join_key_map[join_pair[1].source_name].append(join_pair[1].field_name)
        
        prv_df, prv_join_key = None, None
        for join_pair in path:
            key1, key2 = join_pair[0], join_pair[1]
            tbl1, tbl2 = key1.source_name, key2.source_name

            if key1 == key2:
                attrs_needs = tbl_attrs_proj_map[tbl1]
                return read_csv_columns_with_sampling(self.table_path + tbl1, list(attrs_needs), self.sample_size)
           
            if prv_df is None and prv_join_key is None:
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