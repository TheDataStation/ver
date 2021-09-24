from ddapi import API
from api.apiutils import Relation
from modelstore.elasticstore import StoreHandler
from knowledgerepr.fieldnetwork import deserialize_network
import pandas as pd

data_path = "/home/yuegong/Documents/datasets/adventureWork/"


def is_column_nan(unique_values, col):
    if unique_values != 1:
        return False
    df = pd.read_csv(data_path + col.source_name, usecols=[col.field_name])
    if df[col.field_name].isnull().values.any():
        return True
    return False


def find_join_path(start, max_hop):
    pass


if __name__ == '__main__':
    # create store handler
    store_client = StoreHandler()
    # read graph
    path = 'models/adventureWorks/'
    network = deserialize_network(path)
    api = API(network)
    api.init_store()

    table = 'Employee.csv'
    res = api.drs_from_table(table)
    for el in res:

        unique_values = network.get_size_of(el.nid) # the number of unique values in a column
        cardinality = network.get_cardinality_of(el.nid)    # the number of unique values / the number of values
        total_values = unique_values / cardinality
        print("query col:", el, "unique_values:", unique_values, "total_values", total_values, "cardi:", cardinality)
        if is_column_nan(unique_values, el):
            continue
        neighbors = network.neighbors_id(el, Relation.CONTENT_SIM)
        print("---------------neighbors--------------------")
        for nei in neighbors:
            print(nei)
        print("---------------neighbors-------------------")