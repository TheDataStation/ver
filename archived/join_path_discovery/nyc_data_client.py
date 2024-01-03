from archived.modelstore import StoreHandler
from archived.DoD import data_processing_utils as dpu
from join_path_api import Join_Path_API, get_correlations
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

def output_correlations(api, table):
    api = Join_Path_API(model_path)

    jps = get_join_paths_from(api, table, 1, 100)
    print(jps)
    f = open(table + '_jp_corr.csv', 'w')
    get_correlations(jps, data_path, f)

def get_join_paths_from(api, table, max_hop, offset=0):
    result = []
    api.find_join_paths_from(table, max_hop, result)
    if offset != 0:
        result = result[:offset]
    for jp in result:
        print(jp.to_str())
    return result

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
    # jps = get_join_paths_from(api, 'fxdy-q85h.csv', 1, 100)
    # examples: 'kvhd-5fmu.csv', 'wcmg-48ep.csv'
    output_correlations(api, 'kxua-p5dg.csv')
