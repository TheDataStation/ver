from ddapi import API
from api.apiutils import Relation
from modelstore.elasticstore import StoreHandler
from knowledgerepr.fieldnetwork import deserialize_network
import pandas as pd
from join_path import JoinKey, JoinPath
import sys

if __name__ == "__main__":
    # create store handler
    store_client = StoreHandler()
    # read command line arguments
    data_path = '/home/cc/datasets/adventureWorks/' # path to csv files
    path = '../models/adventureWorks/'  # path = '../models/adventureWorks/'
    table = 'Employee.csv'     # Employee.csv
    max_hop = 2  # max_hop of join paths

    network = deserialize_network(path)
    api = API(network)
    api.init_store()

    columns = api.drs_from_table(table)
    for col in columns:
        df = pd.read_csv(data_path + col.source_name, usecols=[col.field_name])
        res = network.get_non_empty_values_of(col.nid)
        print(col)
        print("gt", df[col.field_name].notnull().sum())
        if not res:
            print('none')
        else:
            print(res)