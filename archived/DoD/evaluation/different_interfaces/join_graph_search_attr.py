import json
from archived.DoD import column_infer
from archived.DoD import ViewSearchPruning
from knowledgerepr import fieldnetwork
from archived.modelstore import StoreHandler
import pandas as pd
import os
import time
from archived.ddapi import API as DDAPI
from aurum_api.algebra import AurumAPI as AurumAPI
from aurum_api.apiutils import Relation
import csv
from aurum_api.apiutils import DRS, Operation, OP

model_path = '/home/cc/models_opendata_05/'
base_path = '/home/cc/opendata_cleaned/'
output_base_path = '/home/cc/generality_experiment/_views_attr/'
query_path = '/home/cc/generality_experiment/queries_attr/'
sep = ','
join_paths = pd.read_csv('/home/cc/join_path_discovery/network_sampled_05_1.csv')
query_not_found_gt_path = output_base_path + 'not_found_gt.txt'
no_gt_log = open(query_not_found_gt_path, 'w', buffering=1)
query_found_gt_path = output_base_path + 'found_gt.txt'
found_gt_log = open(query_found_gt_path, 'w', buffering=1)

store_client = StoreHandler()
# network = None
network = fieldnetwork.deserialize_network(model_path)
dd_api = DDAPI(network)
dd_api.init_store()
aurum_api = AurumAPI(network=network, store_client=store_client)
columnInfer = column_infer.ColumnInfer(network=network, store_client=store_client, csv_separator=sep)
viewSearch = ViewSearchPruning(network=network, store_client=store_client, base_path=base_path, csv_separator=sep)

def read_query(path):
    with open(path) as f:
        attr = f.readline().strip()
        return [attr]

def get_ground_truth(jp_num):
    with open(query_path + 'jp_' + str(jp_num) + '.txt') as f:
        col = f.readline().strip()
        return col

def remove_redundant(drs_all, kw):
    lookup = {}
    for drs in drs_all:
        lookup[drs.nid] = drs
    return DRS(list(lookup.values()), Operation(OP.KW_LOOKUP, params=[kw]))



def query_attrs(jp_name, attrs, output_path, gt_col, csv_writer):
    log_path = output_path + "/log.txt"
    log = open(log_path, "w", buffering=1)
    log_path2 = output_path + "/gt_log.txt"
    log2 = open(log_path2, "w", buffering=1)
    gt_num = -1
    
    candidate_columns = {}
    print("getting candidate columns")
    for i, attr in enumerate(attrs):
        drs_attr = aurum_api.search_attribute(attr, max_results=10000)
        print("before num", len(drs_attr.data))
        print(drs_attr.data)
        drs_attr = remove_redundant(drs_attr, '')
        print("after num", len(drs_attr.data))
        candidate_columns['Column{}'.format(i+1)] = drs_attr

 
    columns = {}
    idx = 0

    gt_columns = []
   
    hits = candidate_columns['Column1']
    visited = set()
    pruned = []
    for hit in hits:
        if hit.source_name in visited:
            continue
        if hit.field_name == gt_col:
            gt_columns.append('Column1')
            log2.write("{} found gt column\n".format('Column1'))
    
        pruned.append(hit)
        columns[hit.source_name] = hit.field_name
        visited.add(hit.source_name)
    
    idx += 1

    if len(gt_columns) < 1:
        no_gt_log.write(jp_name + '\n')
        return
    found_gt_log.write(jp_name + '\n')
    
    i = 0
    cnt = 0
    total = len(columns.keys())
    
    for tbl1, col1 in columns.items():
        cnt += 1
        print("progress: {}/{}".format(cnt, total))
        if col1 == gt_col:
            gt_num = i
            log2.write("groud truth: {}\n".format(gt_num))
        csv_writer.writerow([tbl1, col1, "", "", "", "", 1])
        i += 1
       
    log.write("ground truth join path: {}\n".format(gt_num))
    log.write("total number of join paths: {}\n".format(i))



def _query_attrs(jp_name, attrs, output_path, gt_jp, csv_writer):
    log_path = output_path + "/log.txt"
    log = open(log_path, "w", buffering=1)
    log_path2 = output_path + "/gt_log.txt"
    log2 = open(log_path2, "w", buffering=1)
    gt_num = -1
    
    candidate_columns = {}
    print("getting candidate columns")
    for i, attr in enumerate(attrs):
        drs_attr = aurum_api.search_attribute(attr, max_results=10000)
        print("before num", len(drs_attr.data))
        print(drs_attr.data)
        drs_attr = remove_redundant(drs_attr, '')
        print("after num", len(drs_attr.data))
        candidate_columns['Column{}'.format(i+1)] = drs_attr

 
    columns = [{} for _ in range(2)]
    idx = 0

    gt_columns = []
    for k in ['Column1', 'Column2']:
        hits = candidate_columns[k]
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
    
    for tbl1, col1 in columns[0].items():
        cnt += 1
        print("progress: {}/{}".format(cnt, total))
        if tbl1 in columns[1]:
            col2 = columns[1][tbl1]
            if col1 != col2:
                attrs_project = [col1, col2]
                csv_writer.writerow([tbl1, col1, "", "", attrs_project[0], attrs_project[1], 1])
                i += 1

        cols = dd_api.drs_from_table(tbl1)
        for join_col1 in cols:
            neighbors = network.neighbors_id(join_col1, Relation.CONTENT_SIM)
            for join_col2 in neighbors:
                tbl2 = join_col2.source_name
                if tbl2 not in columns[1]:
                    continue
                
                if (tbl1, join_col1.field_name, tbl2, join_col2.field_name) == gt_jp or (tbl2, join_col2.field_name, tbl1, join_col1.field_name) == gt_jp:
                    gt_num = i
                    log2.write("groud truth: {}\n".format(gt_num))
                    
                col2 = columns[1][tbl2]
                attrs_project = [col1, col2]
                csv_writer.writerow([tbl1, join_col1.field_name, tbl2, join_col2.field_name, attrs_project[0], attrs_project[1], 2])
                i += 1

    log.write("ground truth join path: {}\n".format(gt_num))
    log.write("total number of join paths: {}\n".format(i))


def query_attr_jp(jp_name, jp_num):
    output_path = "{}/{}".format(output_base_path, jp_name[:-4])
    if not os.path.exists(output_path):
        os.makedirs(output_path)
    else:
        print("processed already")
        return    
    network_file = open(output_path + '/join_paths.csv', 'w', encoding='UTF8', buffering=1)
    csv_writer = csv.writer(network_file)
    csv_writer.writerow(['tbl1', 'col1', 'tbl2', 'col2', 'proj1', 'proj2', 'num_relation'])
    
    attrs = read_query(query_path + jp_name)
    print(attrs)
    
    gt_col = get_ground_truth(jp_num)
    
    profile = {"col_sel": 0, "io": 0, "jp_search": 0, "total": 0}
    
    start = time.time()
    query_attrs(jp_name, attrs, output_path, gt_col, csv_writer)
    profile["total"] = time.time() - start
    print("total time:", profile['total'])
    with open('{}/run_time.json'.format(output_path), 'w') as json_file:
        json.dump(profile, json_file)
   
def main():
    for filename in os.listdir(query_path):
        print("processing", filename)
        query_attr_jp(filename, int(filename[3:-4]))

main()