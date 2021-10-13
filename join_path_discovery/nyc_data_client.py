from ddapi import API
from api.apiutils import Relation
from modelstore.elasticstore import StoreHandler
from knowledgerepr.fieldnetwork import deserialize_network
import pandas as pd
from join_path import JoinKey, JoinPath
from DoD import data_processing_utils as dpu
from join_path_api import Join_Path_API
import sys
import os

def get_neighbor_rank(api):
    num_of_neighbors = []
    for table in os.listdir(data_path):
        result = []
        api.find_join_paths_from(table, max_hop, result)
        print(table, len(result))
        num_of_neighbors.append((len(result), table))
    
    sorted_list = sorted(num_of_neighbors, reverse=True)
    f = open('nyc_open_data.csv', 'w')
    f.write("table_name, " + "num_of_neighbors\n")
    for num_nei, table in sorted_list:
        f.write(table + "," + str(num_nei) + "\n")

def get_join_paths_from(api, table, max_hop, offset):
    result = []
    api.find_join_paths_from(table, max_hop, result)
    for jp in result[0:offset]:
        print(jp.to_str())
    return result[0:offset]

def get_dataframe(data_path, tbl, col):
    df = dpu.read_column(data_path + tbl, col)
    print(df)
    return df
if __name__ == "__main__":
    # create store handler
    store_client = StoreHandler()
    # read command line arguments
    data_path = '/home/cc/datasets/nyc_open_data/' # path to csv files
    model_path = '../models/nyc_open_data/'  # path to the graph model
    max_hop = 1  # max_hop of join paths

    api = Join_Path_API(model_path)

    jps = get_join_paths_from(api, 'dd6w-hnq9.csv', 1, 100)