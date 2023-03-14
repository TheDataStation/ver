from archived.DoD import column_infer
from archived.DoD import ViewSearchPruning
from knowledgerepr import fieldnetwork
from archived.modelstore import StoreHandler
import pandas as pd
import os
from archived.ddapi import API
from aurum_api.apiutils import Relation
import csv

model_path = '/home/cc/models_opendata_05/'
base_path = '/home/cc/opendata_cleaned/'
output_base_path = '/home/cc/generality_experiment/views_table/'
query_path = '/home/cc/generality_experiment/queries_table/'
sep = ','
join_paths = pd.read_csv('/home/cc/join_path_discovery/network_sampled_05_1.csv')
query_not_found_gt_path = output_base_path + 'not_found_gt.txt'
no_gt_log = open(query_not_found_gt_path, 'w', buffering=1)
query_found_gt_path = output_base_path + 'found_gt.txt'
found_gt_log = open(query_found_gt_path, 'w', buffering=1)

store_client = StoreHandler()
# network = None
network = fieldnetwork.deserialize_network(model_path)
aurum_api = API(network)
aurum_api.init_store()
columnInfer = column_infer.ColumnInfer(network=network, store_client=store_client, csv_separator=sep)
viewSearch = ViewSearchPruning(network=network, store_client=store_client, base_path=base_path, csv_separator=sep)


def get_ground_truth(jp_num):
    row = join_paths.iloc[jp_num]
    return row['tbl1'], row['col1'], row['tbl2'], row['col2']

def read_query(path):
    with open(path) as f:
        tbl = f.readline().strip()
        return tbl

def query_table(jp_name, tbl, output_path, gt_jp, csv_writer):
    log_path = output_path + "/log.txt"
    log = open(log_path, "w", buffering=1)
    log_path2 = output_path + "/gt_log.txt"
    log2 = open(log_path2, "w", buffering=1)
    gt_num = -1
   
    i = 0
    cols = aurum_api.drs_from_table(tbl)
    for join_col1 in cols:
        neighbors = network.neighbors_id(join_col1, Relation.CONTENT_SIM)
        for join_col2 in neighbors:
            tbl2 = join_col2.source_name
            if (tbl, join_col1.field_name, tbl2, join_col2.field_name) == gt_jp or (tbl2, join_col2.field_name, tbl, join_col1.field_name) == gt_jp:
                gt_num = i
                log2.write("groud truth: {}\n".format(gt_num))
                found_gt_log.write(jp_name + '\n')
            csv_writer.writerow([tbl, join_col1.field_name, tbl2, join_col2.field_name, "", "", 2])
            i += 1

def query_table_jp(jp_name, jp_num):
    output_path = "{}/{}".format(output_base_path, jp_name[:-4])
    if not os.path.exists(output_path):
        os.makedirs(output_path)
    else:
        print("processed already")
        return   
    
    network_file = open(output_path + '/join_paths.csv', 'w', encoding='UTF8', buffering=1)
    csv_writer = csv.writer(network_file)
    csv_writer.writerow(['tbl1', 'col1', 'tbl2', 'col2', 'proj1', 'proj2', 'num_relation'])

    tbl = read_query(query_path + jp_name[:-4] + '.txt')
    print(tbl)

    gt_tbl1, gt_col1, gt_tbl2, gt_col2 = get_ground_truth(jp_num)
    gt_jp = (gt_tbl1, gt_col1, gt_tbl2, gt_col2)
    
    query_table(jp_name, tbl, output_path, gt_jp, csv_writer)
    

def main():
    for filename in os.listdir(query_path):
        print("processing", filename)
        query_table_jp(filename, int(filename[3:-4]))

main()