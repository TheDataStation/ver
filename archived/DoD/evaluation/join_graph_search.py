import json
import server_config as config
from DoD import column_infer
from DoD.utils import FilterType
from DoD.view_search_pruning import ViewSearchPruning
from knowledgerepr import fieldnetwork
from modelstore.elasticstore import StoreHandler
from DoD import data_processing_utils as dpu
import pandas as pd
import os
import errno
import time
from aurum_api.ddapi import API
from api.apiutils import Relation
from view_rank import get_S4_score_direct
import csv

model_path = '/home/cc/models_opendata_05/'
base_path = '/home/cc/opendata_cleaned/'
output_base_path = '/home/cc/output_views_small_col_sel2/'
query_path = '/home/cc/queries_cleaned2/'
sep = ','
join_paths = pd.read_csv('/home/cc/join_path_discovery/network_sampled_05_2.csv')
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

def read_example_values(path):
    df = pd.read_csv(path)
    columns = []
    for (col_name, col_data) in df.iteritems():
        columns.append(col_data.values.tolist()) 
    values = []
    for i in range(len(columns[0])):
        values.append([column[i] for column in columns])
    return values

def column_selection(attrs, values, profile, output_path):
    candidate_columns, sample_score, _, _, _ = columnInfer.infer_candidate_columns(attrs, values, 10000)
    print("getting candidate columns")
    
    start_col_sel = time.time()
    _, _, _ = columnInfer.view_spec_cluster_union_find(candidate_columns, sample_score, output_path)
    end_col_sel = time.time()
    profile["col_sel"] = end_col_sel - start_col_sel
    

def query_examples(jp_name, attrs, values, profile, output_path, gt_jp, csv_writer):
    log_path = output_path + "/log.txt"
    log = open(log_path, "w", buffering=1)
    log_path2 = output_path + "/gt_log.txt"
    log2 = open(log_path2, "w", buffering=1)
    gt_num = -1
    
    candidate_columns, sample_score, hit_type_dict, match_dict, hit_dict = columnInfer.infer_candidate_columns(attrs, values, 10000)
    print("getting candidate columns")
    
    start_col_sel = time.time()
    _, _, results_hit = columnInfer.view_spec_cluster_union_find(candidate_columns, sample_score, output_path)
    end_col_sel = time.time()
    profile["col_sel"] = end_col_sel - start_col_sel
 
    columns = [{} for _ in range(2)]
    idx = 0

    gt_columns = []
    for k in ['Column1', 'Column2']:
        hits = results_hit[k]
        visited = set()
        pruned = []
        for hit in hits:
            if hit.source_name in visited:
                continue
            if k == 'Column1' and hit.source_name == gt_jp[0]:
                gt_columns.append('Column1')
                log2.write("{} found gt column\n".format(k))
            if k == 'Column2' and hit.source_name == gt_jp[2]:
                gt_columns.append('Column2')
                log2.write("{} found gt column\n".format(k))
            pruned.append(hit)
            columns[idx][hit.source_name] = hit.field_name
            visited.add(hit.source_name)
        idx += 1

    if len(gt_columns) != 2:
        no_gt_log.write(jp_name + '\n')
        return
    found_gt_log.write(jp_name + '\n')
    i = 0
    cnt = 0
    total = len(columns[0].keys())
    
    search_start = time.time()
    for tbl1, col1 in columns[0].items():
        cnt += 1
        print("progress: {}/{}".format(cnt, total))
        if tbl1 in columns[1]:
            col2 = columns[1][tbl1]
            if col1 != col2:
                attrs_project = [col1, col2]
                csv_writer.writerow([tbl1, col1, "", "", attrs_project[0], attrs_project[1], 1])
                i += 1

        cols = aurum_api.drs_from_table(tbl1)
        for join_col1 in cols:
            neighbors = network.neighbors_id(join_col1, Relation.CONTENT_SIM)
            for join_col2 in neighbors:
                tbl2 = join_col2.source_name
                if tbl2 not in columns[1]:
                    continue
                
                if (tbl1, join_col1.field_name, tbl2, join_col2.field_name) == gt_jp:
                    gt_num = i
                    log2.write("groud truth: {}\n".format(gt_num))
                    
                col2 = columns[1][tbl2]
                attrs_project = [col1, col2]
                csv_writer.writerow([tbl1, join_col1.field_name, tbl2, join_col2.field_name, attrs_project[0], attrs_project[1], 2])
                i += 1
    profile["jp_search"] = time.time() - search_start
    log.write("ground truth join path: {}\n".format(gt_num))
    log.write("total number of join paths: {}\n".format(i))

def get_ground_truth(jp_num):
    row = join_paths.iloc[jp_num]
    return row['tbl1'], row['col1'], row['tbl2'], row['col2']


def query(jp_name, jp_num):
    output_path = "{}/{}".format(output_base_path, jp_name[:-4])
    if not os.path.exists(output_path):
        os.makedirs(output_path)
    else:
        print("processed already")
        return    
    # network_file = open(output_path + '/join_paths.csv', 'w', encoding='UTF8', buffering=1)
    # csv_writer = csv.writer(network_file)
    # csv_writer.writerow(['tbl1', 'col1', 'tbl2', 'col2', 'proj1', 'proj2', 'num_relation'])
    
    attrs = ["", ""]
    values = read_example_values(query_path + jp_name)
    print(values)
    profile = {"col_sel": 0, "io": 0, "jp_search": 0, "total": 0}
    gt_tbl1, gt_col1, gt_tbl2, gt_col2 = get_ground_truth(jp_num)
    gt_jp = (gt_tbl1, gt_col1, gt_tbl2, gt_col2)
    start = time.time()
    column_selection(attrs, values, profile, output_path)
    query_examples(jp_name, attrs, values, profile, output_path, gt_jp, csv_writer)
    profile["total"] = time.time() - start
    print("total time:", time.time() - start)
    with open('{}/run_time.json'.format(output_path), 'w') as json_file:
        json.dump(profile, json_file)


def main():
    for filename in os.listdir(query_path):
        print("processing", filename)
        query(filename, int(filename[3:-4]))

def main2():
    for filename in os.listdir('/home/cc/output_views_small2/'):
        if filename[0:2] != 'jp':
            continue
        print("processing", filename)
        query(filename + '.csv', int(filename[3:]))
main2()
# print(get_ground_truth(1204))
# query('jp_1204.csv', 1204)
# values = [["rain", "year", "gdf"], ["-  Precipitation Amount (mm)", "-  Year", "-  Greatest daily fall (mm)"]]
# values = [["HMA Egypt", "Louise Ellman", "UKBSC"], ["HMA Egypt", "Dr Sarah Wollaston", "UKBSC"]]