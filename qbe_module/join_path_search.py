from collections import deque, defaultdict
from aurum_api.algebra import AurumAPI
from aurum_api.apiutils import DRS, Relation
from typing import List
from qbe_module.column_selection import Column
from copy import deepcopy


class JoinPath:
    def __init__(self, path: List, tbl_proj_attrs):
        # join path is a list of key pairs: [join_key1, join_key2], [join_key3, join_key4]
        self.path = path
        self.tbl_proj_attrs = tbl_proj_attrs
    
    def reverse(self):
        return JoinPath([sub[::-1] for sub in self.path[::-1]], None)
    
    def to_str(self):
        output = ""
        for i, join_key_pair in enumerate(self.path):
            output += "{}.{} JOIN {}.{}".format(join_key_pair[0].source_name, join_key_pair[0].field_name,
            join_key_pair[1].source_name, join_key_pair[1].field_name)
            if i + 1 < len(self.path):
                output += '\n'
        return output


class JoinPathSearch:
    def __init__(self, aurum_api: AurumAPI):
        self.api = aurum_api
    
    def is_visited(self, x: DRS, path: List) -> bool:
        for join_key_pair in path:
            if join_key_pair[0] == x or join_key_pair[1] == x:
                return True
        return False
      
    def find_join_paths_from_col(self, src: DRS, rel: Relation=Relation.CONTENT_SIM, max_hop: int=1):
        q = deque()
        path = [[src, None]]
        q.append(deepcopy(path))

        result = []
        while q:
            cur_path = q.popleft()
            cur_last = cur_path[-1][0]
            neighbors = self.api.neighbors(cur_last, rel)
           
            for nei in neighbors:
                if not self.is_visited(nei, cur_path):
                    new_path = deepcopy(cur_path)
                    new_path[-1][1] = nei
                    result.append(new_path)
                    if len(new_path) == max_hop:
                        continue
                    nei_tbl = self.api._general_to_drs(nei.source_name)
                    # search next hop
                    for next_src in nei_tbl:
                        next_path = deepcopy(new_path)
                        next_path.append([next_src, None])
                        q.append(next_path)
                    
        return result

    def find_join_paths(self, src, tgt, rel: Relation=Relation.CONTENT_SIM, max_hop: int=1):
        result = []
        for src_col in self.api._general_to_drs(src):
            src_paths = self.find_join_paths_from_col(src_col, rel, max_hop)
            for src_path in src_paths:
                end_tbl = src_path[-1][1].source_name
                if end_tbl == tgt:
                    jp = JoinPath(src_path, [])
                    result.append(jp)
        return result
    
    def find_join_paths_between_two_tbls(self, src_tbl, tgt_tbls, src_dict, tgt_dict, rel: Relation=Relation.CONTENT_SIM, max_hop: int=1):
        result = []
        if src_tbl in tgt_tbls:
            src_tbl_drs = self.api._general_to_drs(src_tbl).data[0]
            jp = JoinPath([[src_tbl_drs, src_tbl_drs]], [src_dict[src_tbl], tgt_dict[src_tbl]])
            # print(f"same table::{jp.to_str()}")
            result.append(jp)
          
        for src_col in self.api._general_to_drs(src_tbl):
            src_paths = self.find_join_paths_from_col(src_col, rel, max_hop)
            for src_path in src_paths:
                end_tbl = src_path[-1][1].source_name
                if end_tbl in tgt_tbls:
                    jp = JoinPath(src_path, [src_dict[src_tbl], tgt_dict[end_tbl]])
                    # print(f"different table::{jp.to_str()}")
                    result.append(jp)
        return result
                
    def find_join_paths_between_two_cols(self, src: Column, tgts: List[Column], rel: Relation=Relation.CONTENT_SIM, max_hop: int=1):
        tgt_tbls = defaultdict(list)
        result = []

        for tgt in tgts:
            tgt_tbls[tgt.tbl_name].append(tgt)

        if src.tbl_name in tgt_tbls:
            for tgt in tgt_tbls[src.tbl_name]:
                result.append(JoinPath([[src.drs, src.drs]], [src.key(), tgt.key()]))

        
        src_tbl = self.api._general_to_drs(src.tbl_name)
        for src_col in src_tbl:
            src_paths = self.find_join_paths_from_col(src_col, rel, max_hop)
            for src_path in src_paths:
                if src_path[-1][1].source_name in tgt_tbls:
                    for tgt in tgt_tbls[src_path[-1][1].source_name]:
                        result.append(JoinPath(src_path, [src.key(), tgt.key()]))
        
        return result

    def is_column_nan(self, col: DRS):
        non_empty = self.network.get_non_empty_values_of(col.nid)
        if non_empty == 0:
            return True
        return False