import json
from archived.DoD import column_infer
from archived.DoD import ViewSearchPruning
from knowledgerepr import fieldnetwork
from archived.modelstore import StoreHandler
from archived.DoD import data_processing_utils as dpu
import pandas as pd
import os
import time
from archived.ddapi import API
from aurum_api.apiutils import Relation

model_path = '/home/cc/models_opendata_05/'
base_path = '/home/cc/opendata_cleaned/'
output_base_path = '/home/cc/output_views3/'
query_path = '/home/cc/queries100/'
sep = ','
join_paths = pd.read_csv('/home/cc/join_path_discovery/network_sampled_05.csv')
sample_size = 500

store_client = StoreHandler()
# network = None
network = fieldnetwork.deserialize_network(model_path)
aurum_api = API(network)
aurum_api.init_store()
columnInfer = column_infer.ColumnInfer(network=network, store_client=store_client, csv_separator=sep)
viewSearch = ViewSearchPruning(network=network, store_client=store_client, base_path=base_path, csv_separator=sep)
view_cache = {}

def read_example_values(path):
    df = pd.read_csv(path)
    columns = []
    for (col_name, col_data) in df.iteritems():
        columns.append(col_data.values.tolist()) 
    values = []
    for i in range(len(columns[0])):
        values.append([column[i] for column in columns])
    return values

def join_two_tables(tbl1, col1, tbl2, col2):
    io_time = 0
    print("reading tables")

    if tbl1 in view_cache:
        print("Hit Cache!")
        df1 = view_cache[tbl1]
    else:
        io_start = time.time()
        df1 = pd.read_csv(base_path + tbl1)
        io_time += time.time() - io_start
        if len(df1.index) > 1000:
            df1 = df1.sample(1000)
        view_cache[tbl1] = df1

    if tbl2 in view_cache:
        print("Hit Cache!")
        df2 = view_cache[tbl2]
    else:
        io_start = time.time()
        df2 = pd.read_csv(base_path + tbl2)
        io_time += time.time() - io_start
        if len(df2.index) > 1000:
            df2 = df2.sample(1000)
        view_cache[tbl2] = df2
    

    print("begin join")
    
    col_names1, col_names2 = df1.columns, df2.columns
    old_col_names1, old_col_names2 = col_names1, col_names2
    col_names1 = [s + '_x' for s in col_names1]
    col_names2 = [s + '_y' for s in col_names2]
    df1.columns = col_names1
    df2.columns = col_names2
    col1 = col1 + '_x'
    col2 = col2 + '_y'
    df1[col1] = df1[col1].astype(str)
    df2[col2] = df2[col2].astype(str)
    if col1 == col2:
        merged = df1.merge(df2.dropna(subset=[col1]), how='inner', on=col1)
    else:
        merged = df1.merge(df2.dropna(subset=[col2]), how='inner', left_on=col1, right_on=col2)
    df1.columns, df2.columns = old_col_names1, old_col_names2
    return merged, io_time

def query_examples(attrs, values, profile, output_path, gt_jp):
    log_path = output_path + "/log.txt"
    log = open(log_path, "w", buffering=1)
    log_path2 = output_path + "/gt_log.txt"
    log2 = open(log_path2, "w", buffering=1)
    gt_num = -1
    
    candidate_columns, sample_score, hit_type_dict, match_dict, hit_dict = columnInfer.infer_candidate_columns(attrs, values, 10000)
    print("getting candidate columns")
    
    start_col_sel = time.time()
    results, _, results_hit = columnInfer.view_spec_cluster2(candidate_columns, sample_score, output_path)
    print(results_hit.keys())
    columns = [{} for _ in range(2)]
    idx = 0

    for k in ['Column1', 'Column2']:
        hits = results_hit[k]
        visited = set()
        pruned = []
        print("before", len(hits))
        for hit in hits:
            if hit.source_name in visited:
                continue
            if hit.source_name == gt_jp[0]:
                log2.write("{} found gt column\n".format(k))
            if hit.source_name == gt_jp[2]:
                log2.write("{} found gt column\n".format(k))
            pruned.append(hit)
            columns[idx][hit.source_name] = hit.field_name
            visited.add(hit.source_name)
        print("after", k, len(pruned))
        idx += 1
    end_col_sel = time.time()
    profile["col_sel"] = end_col_sel - start_col_sel

    i = 0
    cnt = 0
    total = len(columns[0].keys())

    total_io_time = 0
    total_s4_time = 0

    start_join = time.time()
    for tbl1, col1 in columns[0].items():
        cnt += 1
        print("progress: {}/{}".format(cnt, total))
        if tbl1 in columns[1]:
            col2 = columns[1][tbl1]
            if col1 != col2:
                attrs_project = [col1, col2]
                if tbl1 in view_cache:
                    mjp = view_cache[tbl1]
                    print("HIT Cached View")
                else:
                    io_start = time.time()
                    mjp = dpu.read_relation(base_path + "/" + tbl1)
                    total_io_time += time.time() - io_start
                    view_cache[tbl1] = mjp
                print("project {} from {}".format(attrs_project, tbl1))
                proj_view = dpu.project(mjp, attrs_project)
                proj_view = proj_view.drop_duplicates().head(2000)
                # s4_start = time.time()
                # s4_score = get_S4_score_direct(proj_view, values, 1)
                # total_s4_time += time.time() - s4_start
                start_save = time.time()
                view_path = output_path + "/view_" + str(i) + ".csv"
                proj_view.to_csv(view_path, encoding='utf8', index=False)
                log.write("view_{}.csv\n".format(i))
                log.write("relations: {}\n".format(1))
                log.write("{}\n".format(tbl1))
                log.write("{}\n".format(attrs_project))
                # log.write("s4_score: {}\n".format(s4_score))
                print("saved {}".format(view_path))
                total_io_time += time.time() - start_save
                i += 1

        cols = aurum_api.drs_from_table(tbl1)
        for join_col1 in cols:
            neighbors = network.neighbors_id(join_col1, Relation.CONTENT_SIM)
            for join_col2 in neighbors:
                tbl2 = join_col2.source_name
                if tbl2 not in columns[1]:
                    continue
                print("joining {}.{} with {}.{}".format(tbl1, join_col1.field_name, tbl2, join_col2.field_name))
                col2 = columns[1][tbl2]
                attrs_project = [col1+'_x', col2+'_y']
                mjp, io_time = join_two_tables(tbl1, join_col1.field_name, tbl2, join_col2.field_name, attrs_project)
                total_io_time += io_time
                print("finish join")
                num_rows = len(mjp.index)
                if (tbl1, join_col1.field_name, tbl2, join_col2.field_name) == gt_jp:
                    log2.write("found groud truth!!\n")
                    if num_rows != 0:
                        gt_num = i
                    else:
                        log2.write("found but sampled join gives empty view\n")
                if num_rows == 0:
                    print("empty view!")
                    continue
                
                proj_view = dpu.project(mjp, attrs_project)
                proj_view = proj_view.drop_duplicates().head(2000)
                # s4_start = time.time()
                # s4_score = get_S4_score_direct(proj_view, values, 2)
                # total_s4_time += time.time() - s4_start
                start_save = time.time()
                view_path = output_path + "/view_" + str(i) + ".csv"
                proj_view.to_csv(view_path, encoding='utf8', index=False)
                print("saved {}".format(view_path))
                log.write("view_{}.csv\n".format(i))
                log.write("{}, {} JOIN {}, {}\n".format(tbl1, join_col1.field_name, tbl2, join_col2.field_name))
                log.write("relations: {}\n".format(2))
                log.write("{}\n".format(attrs_project))
                # log.write("s4_score: {}\n".format(s4_score))
                total_io_time += time.time() - start_save
                i += 1
    end_join = time.time()
    profile["materialization"] = end_join - start_join
    profile["io"] = total_io_time
    profile["s4"] = total_s4_time
    log.write("ground truth join path: {}\n".format(gt_num))
    log.write("total number of views: {}\n".format(i))

def get_ground_truth(jp_num):
    row = join_paths.iloc[jp_num]
    return row['tbl1'], row['col1'], row['tbl2'], row['col2']

def query(jp_name, jp_num):
    output_path = "{}/{}".format(output_base_path, jp_name[:-4])
    if not os.path.exists(output_path):
        os.makedirs(output_path)
    else:
        print("processed before")
        return
    attrs = ["", ""]
    values = read_example_values(query_path + jp_name)
    print(values)
    profile = {"col_sel": 0, "io": 0, "materialization": 0, "s4": 0, "total": 0}
    gt_tbl1, gt_col1, gt_tbl2, gt_col2 = get_ground_truth(jp_num)
    gt_jp = (gt_tbl1, gt_col1, gt_tbl2, gt_col2)
    start = time.time()
    query_examples(attrs, values, profile, output_path, gt_jp)
    profile["total"] = time.time() - start
    print("total time:", time.time() - start)
    with open('{}/run_time.json'.format(output_path), 'w') as json_file:
        json.dump(profile, json_file)


def main():
    for filename in os.listdir(query_path):
        print("processing", filename)
        query(filename, int(filename[3:-4]))

main()
# query('jp_1648.csv', 1648)
# values = [["rain", "year", "gdf"], ["-  Precipitation Amount (mm)", "-  Year", "-  Greatest daily fall (mm)"]]
# values = [["HMA Egypt", "Louise Ellman", "UKBSC"], ["HMA Egypt", "Dr Sarah Wollaston", "UKBSC"]]