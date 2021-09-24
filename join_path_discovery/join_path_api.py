from ddapi import API
from api.apiutils import Relation
from modelstore.elasticstore import StoreHandler
from knowledgerepr.fieldnetwork import deserialize_network
import pandas as pd
from join_path import JoinKey, JoinPath

data_path = "/home/yuegong/Documents/datasets/adventureWork/"


def is_column_nan(unique_values, col):
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


def find_join_path(start, max_hop, result):
    if max_hop == 0:
        return
    columns = api.drs_from_table(start)
    for col in columns:
        unique, total = get_sizes_from_drs(col)
        path = [JoinKey(col, unique, total)]
        unique_values = network.get_size_of(col.nid)
        if not is_column_nan(unique_values, col):
            neighbors = network.neighbors_id(col, Relation.CONTENT_SIM)
            for nei in neighbors:
                unique, total = get_sizes_from_drs(nei)
                path.append(JoinKey(nei, unique, total))
                result.append(JoinPath(path[:]))
                find_join_path(nei, max_hop-1, result)
                path.pop()


if __name__ == '__main__':
    # create store handler
    store_client = StoreHandler()
    # read graph
    path = '../models/adventureWorks/'
    network = deserialize_network(path)
    api = API(network)
    api.init_store()

    table = 'Employee.csv'
    result = []
    find_join_path(table, 1, result)
    for jp in result:
        print(jp)
    # res = api.drs_from_table(table)
    # for el in res:
    #
    #     unique_values = network.get_size_of(el.nid) # the number of unique values in a column
    #     cardinality = network.get_cardinality_of(el.nid)    # the number of unique values / the number of values
    #     total_values = unique_values / cardinality
    #     print("query col:", el, "unique_values:", unique_values, "total_values", total_values, "cardi:", cardinality)
    #     if is_column_nan(unique_values, el):
    #         continue
    #     neighbors = network.neighbors_id(el, Relation.CONTENT_SIM)
    #     print("---------------neighbors--------------------")
    #     for nei in neighbors:
    #         print(nei)
    #     print("---------------neighbors-------------------")