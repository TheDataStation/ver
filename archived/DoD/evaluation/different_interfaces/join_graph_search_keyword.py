import json
from archived.DoD import column_infer
from archived.DoD import ViewSearchPruning
from knowledgerepr import fieldnetwork
from archived.modelstore import StoreHandler
import pandas as pd
import os
import time
from archived.ddapi import API
import csv

model_path = '/home/cc/models_opendata_05/'
base_path = '/home/cc/opendata_cleaned/'
output_base_path = '/home/cc/generality_experiment/views_keyword/'
query_path = '/home/cc/generality_experiment/queries_keyword/'
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
    with open(query_path + 'jp_' + str(jp_num) + '.txt') as f:
        tbl = f.readline().strip()
        return tbl
    # row = join_paths.iloc[jp_num]
    # return row['tbl1'], row['col1'], row['tbl2'], row['col2']

def read_example_values(path):
    df = pd.read_csv(path)
    columns = []
    for (col_name, col_data) in df.iteritems():
        columns.append(col_data.values.tolist()) 
    values = []
    for i in range(len(columns[0])):
        values.append([column[i] for column in columns])
    return values

def query_keywords(jp_name, attrs, values, profile, output_path, gt_tbl, csv_writer):
    log_path = output_path + "/log.txt"
    log = open(log_path, "w", buffering=1)
    log_path2 = output_path + "/gt_log.txt"
    log2 = open(log_path2, "w", buffering=1)
    gt_num = -1
    
    candidate_columns, sample_score, hit_type_dict, match_dict, hit_dict = columnInfer.infer_candidate_columns(attrs, values, 10000)
    
    hits = candidate_columns['Column1']
    hits_score = sample_score['Column1']
    gt_columns = []
    columns = {}
    
    visited = set()
    pruned = []
    log2.write('number of columns before: {}\n'.format(len(hits.data)))
    for hit in hits:
        if hit.source_name in visited:
            continue
        if hit.source_name == gt_tbl:
            gt_columns.append('Column1')
            log2.write("{} found gt column\n".format('Column1'))
        pruned.append(hit)
        columns[hit.source_name] = hit.field_name
        visited.add(hit.source_name)
    log2.write('number of columns after: {}\n'.format(len(pruned)))

    if len(gt_columns) < 1:
        no_gt_log.write(jp_name + '\n')
        return
    found_gt_log.write(jp_name + '\n')

    i = 0
    cnt = 0
    total = len(columns.keys())
    
    search_start = time.time()
    for tbl1, col1 in columns.items():
        cnt += 1
        print("progress: {}/{}".format(cnt, total))
        if tbl1 == gt_tbl:
            gt_num = i
            log2.write("groud truth: {}\n".format(gt_num))

        csv_writer.writerow([tbl1, col1, "", "", "", "", 1, hits_score[(tbl1, col1)]])
        i += 1

        # cols = aurum_api.drs_from_table(tbl1)
        # for join_col1 in cols:
        #     neighbors = network.neighbors_id(join_col1, Relation.CONTENT_SIM)
        #     for join_col2 in neighbors:
        #         tbl2 = join_col2.source_name
        #         if (tbl1, join_col1.field_name, tbl2, join_col2.field_name) == gt_jp or (tbl2, join_col2.field_name, tbl1, join_col1.field_name) == gt_jp:
        #             gt_num = i
        #             log2.write("groud truth: {}\n".format(gt_num))
                    
        #         csv_writer.writerow([tbl1, join_col1.field_name, tbl2, join_col2.field_name, "", "", 2])
        #         i += 1
    profile["jp_search"] = time.time() - search_start
    log.write("ground truth join path: {}\n".format(gt_num))
    log.write("total number of join paths: {}\n".format(i))

def query_keywords_jp(jp_name, jp_num):
    output_path = "{}/{}".format(output_base_path, jp_name[:-4])
    if not os.path.exists(output_path):
        os.makedirs(output_path)
    else:
        print("processed already")
        return    
    network_file = open(output_path + '/join_paths.csv', 'w', encoding='UTF8', buffering=1)
    csv_writer = csv.writer(network_file)
    csv_writer.writerow(['tbl1', 'col1', 'tbl2', 'col2', 'proj1', 'proj2', 'num_relation', 'score'])
    
    attrs = [""]
    values = read_example_values(query_path + jp_name)
    print(values)
    profile = {"col_sel": 0, "io": 0, "jp_search": 0, "total": 0}
    # gt_tbl1, gt_col1, gt_tbl2, gt_col2 = get_ground_truth(jp_num)
    gt_tbl = get_ground_truth(jp_num)
    # gt_jp = (gt_tbl1, gt_col1, gt_tbl2, gt_col2)
    start = time.time()
    # query_keywords(jp_name, attrs, values, profile, output_path, gt_jp, csv_writer)
    query_keywords(jp_name, attrs, values, profile, output_path, gt_tbl, csv_writer)
    profile["total"] = time.time() - start
    print("total time:", time.time() - start)
    with open('{}/run_time.json'.format(output_path), 'w') as json_file:
        json.dump(profile, json_file)

def main():
    for filename in os.listdir(query_path):
        print("processing", filename)
        if filename[-3:] == 'csv':
            query_keywords_jp(filename, int(filename[3:-4]))

main()