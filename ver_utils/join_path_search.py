from ddapi import API
from api.apiutils import DRS, Relation
from modelstore.elasticstore import StoreHandler
from knowledgerepr.fieldnetwork import deserialize_network
from join_path import JoinPath
import sys


class JoinPathSearch:
    def __init__(self, model_path):
        self.network = deserialize_network(model_path)
        self.api = API(self.network)
        self.api.init_store()
    
    def find_join_path_from_col(self, col: DRS, rel: Relation, max_hop: int, result: list[JoinPath], cur_path: JoinPath):
        if max_hop == 0:
            return
        if not self.is_column_nan(col):
            neighbors = self.network.neighbors_id(col, rel)
            for nei in neighbors:
                cur_path.append(nei)
                result.append(JoinPath(cur_path[:]))
                self.find_join_path_from_col(nei, max_hop - 1, result, cur_path)
                cur_path.pop()

    def find_join_paths_from_tbl(self, start: str, max_hop:int, result: list[JoinPath]):
        columns = self.api.drs_from_table(start)
        for col in columns:
            self.find_join_path_from_col(col, max_hop, result, [col])
    
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