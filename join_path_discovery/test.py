from ddapi import API
from api.apiutils import Relation
from modelstore.elasticstore import StoreHandler
from knowledgerepr.fieldnetwork import deserialize_network
import pandas as pd
from join_path import JoinKey, JoinPath
from DoD import data_processing_utils as dpu
import sys
import os

def get_column_content(sn, fn):
    print("reading", sn, fn)
    df = dpu.read_column(sn, fn)
    return df

if __name__ == "__main__":
    # create store handler
    store_client = StoreHandler()
    # read command line arguments
    data_path = '/home/cc/datasets/nyc_open_data/' # path to csv files
    path = '../models/nyc_open_data/'  # path = '../models/adventureWorks/'
    # table = 'Employee.csv'     # Employee.csv
    max_hop = 1  # max_hop of join paths

    network = deserialize_network(path)
    api = API(network)
    api.init_store()

    num_of_neighbors = []
    for table in os.listdir(data_path):
        result = []
        find_join_paths_from(table, max_hop, result)
        num_of_neighbors.append((len(result), table))
    
    sorted_list = sorted(num_of_neighbors, reverse=True)
    f = open('nyc_open_data.txt', 'w')
    for num_nei, table in sorted_list:
        f.write(table + " " + str(num_nei) + "\n")
