from ddapi import API
from api.apiutils import Relation
from modelstore.elasticstore import StoreHandler
from knowledgerepr.fieldnetwork import deserialize_network
import pandas as pd
from join_path import JoinKey, JoinPath
import sys

# data_path = "/home/yuegong/Documents/datasets/adventureWork/"


def is_column_nan(col):
    unique_values = network.get_size_of(col.nid)
    if unique_values != 1:
        return False
    df = pd.read_csv(data_path + col.source_name, usecols=[col.field_name])
    if df[col.field_name].isnull().values.any():
        return True
    return False


def join_path_formatter(join_path):
    format_str = ""
    for i, join_key in enumerate(join_path):
        format_str += join_key.tbl[:-4] + '.' + join_key.col
        if i < len(join_path)-1:
            format_str += " JOIN "
    return format_str


def get_sizes_from_drs(col):
    unique = network.get_size_of(col.nid)
    total = int(network.get_size_of(col.nid)/network.get_cardinality_of(col.nid))
    return unique, total


def col_to_join_key(col):
    unique, total = get_sizes_from_drs(col)
    return JoinKey(col, unique, total)


def find_join_paths_from(start, max_hop, result):
    columns = api.drs_from_table(start)
    for col in columns:
        find_join_path(col, max_hop, result, [col_to_join_key(col)])


def find_join_path(col, max_hop, result, cur_path):
    if max_hop == 0:
        return
    if not is_column_nan(col):
        neighbors = network.neighbors_id(col, Relation.CONTENT_SIM)
        for nei in neighbors:
            cur_path.append(col_to_join_key(nei))
            result.append(JoinPath(cur_path[:]))
            find_join_path(nei, max_hop - 1, result, cur_path)
            cur_path.pop()


if __name__ == '__main__':
    # create store handler
    store_client = StoreHandler()
    # read command line arguments
    data_path = sys.argv[1] # path to csv files
    path = sys.argv[2]  # path = '../models/adventureWorks/'
    table = sys.argv[3]     # Employee.csv
    max_hop = int(sys.argv[4])   # max_hop of join paths

    network = deserialize_network(path)
    api = API(network)
    api.init_store()

    # find join paths
    result = []
    find_join_paths_from(table, max_hop, result)
    print("# join paths:", len(result))
    for jp in result:
        jp.print_metadata_str()
        print("------------------------")