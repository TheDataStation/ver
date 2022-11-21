from collections import deque, defaultdict
from ddapi import API
from api.apiutils import DRS, Relation
from modelstore.elasticstore import StoreHandler
from join_path import JoinPath
import sys


class JoinPathSearch:
    def __init__(self, aurum_api: API):
        self.api = aurum_api
    
    def is_visited(self, x: DRS, path: list[DRS]) -> bool:
        for node in path:
            if node == x:
                return True
        return False

    def find_join_paths_from_col(self, src: DRS, rel: Relation, max_hop: int):
        q = deque()
        path = [src]
        q.append(path.copy())

        result = []
        for _ in range(max_hop):
            result.append(defaultdict(list))
        while q:
            cur_path = q.popleft()
            if len(cur_path) == max_hop:
                break
            cur_last = cur_path[-1]
            neighbors = self.api.neighbors(cur_last, rel)
            for nei in neighbors:
                if not self.is_visited(nei, cur_path):
                    newpath = cur_path.copy()
                    newpath.append(nei)
                    q.append(newpath)
                    result_map = result[len(newpath)-1]
                    result_map[newpath[-1]].append(newpath)
                    
        return result

    def find_join_paths(self, src: DRS, tgt: DRS, rel: Relation, max_hop: int):
        src_paths = self.find_join_paths_from_col(src, rel, max_hop//2)
        tgt_paths = self.find_join_paths_from_col(tgt, rel, max_hop-max_hop//2)
        result = []

        for src_map in src_paths:
            for tgt_map in tgt_paths:
                for src_key in src_map.keys():
                    if src_key in tgt_map:
                        for src_path in src_map[src_key]:
                            for tgt_path in tgt_map[src_key]:
                                path = src_path + tgt_path.reverse()
                                result.append(path)
        
        return result

    def is_column_nan(self, col: DRS):
        non_empty = self.network.get_non_empty_values_of(col.nid)
        if non_empty == 0:
            return True
        return False


if __name__ == '__main__':
    # create store handler
    store_client = StoreHandler()
    # read command line arguments
    model_path = sys.argv[1]  # path = '../models/adventureWorks/'
    table = sys.argv[2]     # Employee.csv
    max_hop = int(sys.argv[3])   # max_hop of join paths

    jp_client = JoinPathSearch(model_path)

    # find join paths
    result = []
    jp_client.find_join_paths_from(table, max_hop, result)
    print("# join paths:", len(result))
    for jp in result:
        jp.to_str()
        print("------------------------")