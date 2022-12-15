from collections import deque, defaultdict
from algebra import API
from api.apiutils import DRS, Relation
from typing import List
from ver_utils.column_selection import Column

# todo:
# use join path class, write comments
class JoinPath:
    pass

class JoinPathSearch:
    def __init__(self, aurum_api: API):
        self.api = aurum_api
    
    def is_visited(self, x: DRS, path: List[DRS]) -> bool:
        for node in path:
            if node == x:
                return True
        return False
    
    def find_join_paths_from_col(self, src: DRS, rel: Relation=Relation.CONTENT_SIM, max_hop: int=2):
        q = deque()
        path = [src]
        q.append(path.copy())

        result = []
        while q:
            cur_path = q.popleft()
            if len(cur_path)-1 == max_hop:
                break
            cur_last = cur_path[-1]
            neighbors = self.api.neighbors(cur_last, rel)
            for nei in neighbors:
                if not self.is_visited(nei, cur_path):
                    newpath = cur_path.copy()
                    newpath.append(nei)
                    q.append(newpath)
                    result.append(newpath)
                    
        return result

    def find_join_paths_between_two_cols(self, src: Column, tgts: List[Column], rel: Relation=Relation.CONTENT_SIM, max_hop: int=2):
        tgt_tbls = defaultdict(list)
        result = []

        for tgt in tgts:
            tgt_tbls[tgt.tbl_name].append(tgt)

        if src.tbl_name in tgt_tbls:
            for tgt in tgt_tbls[src.tbl_name]:
                result.append(([src.drs], [src.key(), tgt.key()]))
        
        
        src_tbl = self.api.drs_expand_to_table(src.drs)
        for src_col in src_tbl:
            src_paths = self.find_join_paths_from_col(src_col, rel, max_hop)
            for src_path in src_paths:
                if src_col.field_name == 'CountryRegionCode':
                    print(src_path[-1])
                if src_path[-1].source_name in tgt_tbls:
                    for tgt in tgt_tbls[src_path[-1].source_name]:
                        result.append((src_path, [src.key(), tgt.key()]))
        
        return result

    def is_column_nan(self, col: DRS):
        non_empty = self.network.get_non_empty_values_of(col.nid)
        if non_empty == 0:
            return True
        return False
