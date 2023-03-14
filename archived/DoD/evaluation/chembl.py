import json
import server_config as config
from archived.DoD import column_infer
from archived.DoD import FilterType
from archived.DoD import ViewSearchPruning
from knowledgerepr import fieldnetwork
from archived.modelstore import StoreHandler
from archived.DoD import data_processing_utils as dpu
from view_rank import get_S4_score_direct
import os
import errno
import time

model_path = config.Chembl.path_model
sep = config.Chembl.separator
base_outpath = config.Chembl.output_path
base_path = config.Chembl.base_path

store_client = StoreHandler()
network = fieldnetwork.deserialize_network(model_path)
columnInfer = column_infer.ColumnInfer(network=network, store_client=store_client, csv_separator=sep)
viewSearch = ViewSearchPruning(network=network, store_client=store_client, base_path=base_path, csv_separator=sep)

def loadData(path):
    data = json.load(open(path))
    zero_noise_samples = data["zero"]
    mid_noise_samples = data["mid"]
    high_noise_samples = data["high"]
    gt_cols = [(data["table1"], data["attr1"]), (data["table2"], data["attr2"])]
    gt_path = data["jp"]
    return zero_noise_samples, mid_noise_samples, high_noise_samples, gt_cols, gt_path

def loadData_simple(path):
    data = json.load(open(path))
    zero_noise_samples = data["zero"]
    return zero_noise_samples

def loadData2(path):
    data = json.load(open(path))
    zero_noise_samples = data["zero"]
    gt_cols = [(data["table1"], data["attr1"]), (data["table2"], data["attr2"])]
    gt_path = data["jp"]
    return zero_noise_samples, gt_cols, gt_path

def found_gt_view_or_not(results, gt_cols):
    found = True
    for i, result in enumerate(results):
        if gt_cols[i] not in result:
            found = False
            break
    return found

def column_to_drs(table, attr):
    res = columnInfer.aurum_api.search_exact_attribute(attr)
    drs = None
    for x in res.data:
        if x.source_name == table:
            drs = x
    return drs


def evaluate_view_search(es_time, values, vs, ci, candidate_columns, sample_score, hit_dict, flag, gt_cols, gt_path, offset=1000, output_path=""):
    filter_drs = {}
    perf_stats = dict()
    results = []
    dod_rank = False

    pipe_start = time.time()
    if flag == 1:
        idx = 0
        for column, candidates in candidate_columns.items():
            results.append([(c.source_name, c.field_name) for c in candidates])
            filter_drs[(column, FilterType.ATTR, idx)] = candidates
            idx += 1

        found = found_gt_view_or_not(results, gt_cols)

    elif flag == 2:
        results = ci.view_spec_benchmark(sample_score)
        idx = 0
        for column, _ in candidate_columns.items():
            filter_drs[(column, FilterType.ATTR, idx)] = [hit_dict[x] for x in results[idx]]
            idx += 1
        found = found_gt_view_or_not(results, gt_cols)
    elif flag == 3:
        dod_rank = True
        results, _, results_hit = ci.view_spec_cluster2(candidate_columns, sample_score, output_path)
        idx = 0
        for column, _ in candidate_columns.items():
            filter_drs[(column, FilterType.ATTR, idx)] = results_hit[idx]
            idx += 1

        # found = found_gt_view_or_not(results, gt_cols)
        found = True
        print("finish view spec")

    perf_stats["spec_time"] = time.time() - pipe_start

    i = 0

    if not os.path.exists(os.path.dirname(output_path)):
        try:
            os.makedirs(os.path.dirname(output_path))
        except OSError as exc:  # Guard against race condition
            if exc.errno != errno.EEXIST:
                raise
    log_path = output_path + "/log.txt"
    log = open(log_path, "w", buffering=1)
    log.write("Method" + str(flag) + "\n")
    if found:
        log.write("found ground truth\n")
    else:
        log.write("not found ground truth\n")

    gt_view = 0
    perf_stats["ranking_time"] = 0
    perf_stats["flush_time"] = 0
    print("start view search")
    for mjp, attrs_project, metadata, jp, relations, jp_score in vs.search_views({}, filter_drs, perf_stats, max_hops=2,
                                                            debug_enumerate_all_jps=False, offset=offset, dod_rank=dod_rank):
        # if len(attrs_project) != 2:
        #     continue
        try:
            log.write("view" + str(i) + "\n")
            log.write(jp + "\n")
            log.write(str(attrs_project) + "\n")
            log.write("#relations: " + str(relations) + "\n")
        except:
            print("utf8 failed")
            continue
        print(jp)
        print(gt_path)
        gt_attrs = []
        for attr in gt_cols:
            gt_attrs.append(attr[1])
        print(attrs_project)
        print(gt_attrs)
        if jp == gt_path and attrs_project[0] in gt_attrs and attrs_project[1] in gt_attrs:
            gt_view = i

        proj_view = dpu.project(mjp, attrs_project).drop_duplicates()
        if relations == 1:
            proj_view = proj_view.head(2000).drop_duplicates()
        full_view = False
        if dod_rank is False:
            rank_start = time.time()
            score = get_S4_score_direct(proj_view, values, relations)
            perf_stats["ranking_time"] += time.time() - rank_start
            log.write("s4_score: " + str(score) + "\n")
        else:
            rank_start = time.time()
            score = get_S4_score_direct(proj_view, values, relations)
            perf_stats["ranking_time"] += time.time() - rank_start
            log.write("s4_score: " + str(score) + ", ")
            log.write("dod_score: " + str(jp_score) + "\n")

        output_start = time.time()
        if output_path is not None:
            if full_view:
                view_path = output_path + "/raw_view_" + str(i)
                mjp.to_csv(view_path, encoding='utf8', index=False)
            view_path = output_path + "/view_" + str(i) + ".csv"
            proj_view.to_csv(view_path, encoding='utf8', index=False)  # always store this
        perf_stats["flush_time"] += time.time() - output_start
        i += 1
    log.write("total views:" + str(i) + "\n")
    log.write("ground truth view: " +  "view" + str(gt_view) + ".csv" + "\n")
    log.write("#candidate group:" + str(perf_stats["num_candidate_groups"]) + "\n")
    log.write("#join_graphs:" + str(perf_stats["num_join_graphs"]) + "\n")
    log.write("total_time: " + str(time.time() - pipe_start + es_time - perf_stats["flush_time"]) +"\n")
    log.write("spec_time: " + str(es_time + perf_stats["spec_time"]) + "\n")
    if dod_rank:
        log.write("search_time: " + str(perf_stats["search_time"]) + "\n")
    else:
        log.write("search_time: " + str(perf_stats["search_time"]) + "\n")
    log.write("ranking_time: " + str(perf_stats["ranking_time"]) + "\n")
    log.write("flush_time: " + str(perf_stats["flush_time"]))
    vs.clear_all_cache()
    return found

def test_different_sample_size():
    for num in range(1,5):
        zero_noise_samples, gt_cols, gt_path = loadData2("/home/cc/tests_chembl_diff_sample_size_3/chembl_gt" + str(num) + ".json")
        name = "chembl_gt" + str(num) + "/"
        for (idx, values) in enumerate(zero_noise_samples):
            print("Processing query with", idx+1, "samples")
            attrs = ["", ""]
            es_start = time.time()
            candidate_columns, sample_score, hit_type_dict, match_dict, hit_dict = columnInfer.infer_candidate_columns(attrs, values, 600)
            es_time = time.time() - es_start
            evaluate_view_search(es_time, values, viewSearch, columnInfer, candidate_columns, sample_score, hit_dict, 3, gt_cols, gt_path, 1000, base_outpath + name + "sample" + str(idx) + "/")

def test_different_col_size():
    zero_noise_samples = loadData_simple("/home/cc/tests_chembl_diff_col_size/chembl_gt0.json")
    for idx, values in enumerate(zero_noise_samples):
        print("Processing query with", idx + 2, "cols")
        attrs = ["" for _ in range(len(values[0]))]
        es_start = time.time()
        candidate_columns, sample_score, hit_type_dict, match_dict, hit_dict = columnInfer.infer_candidate_columns(
            attrs, values, 600)
        es_time = time.time() - es_start
        evaluate_view_search(es_time, values, viewSearch, columnInfer, candidate_columns, sample_score, hit_dict, 3,
                             [], "", 1000, base_outpath + "sample" + str(idx) + "/")

if __name__ == '__main__':
    for num in range(5):
        zero_noise_samples, mid_noise_samples, high_noise_samples, gt_cols, gt_path = loadData("/home/cc/tests_chembl_5_4/chembl_gt" + str(num) + ".json")
        found = dict()
        found["method1"] = 0
        found["method2"] = 0
        found["method3"] = 0
        name = "chembl_gt" + str(num) + "/"
        for (idx, values) in enumerate(zero_noise_samples):
            print("Processing zero noise samples no.", idx)
            attrs = ["", ""]
            es_start = time.time()
            candidate_columns, sample_score, hit_type_dict, match_dict, hit_dict = columnInfer.infer_candidate_columns(attrs, values, 600)
            es_time = time.time() - es_start
            if evaluate_view_search(es_time, values, viewSearch, columnInfer, candidate_columns, sample_score, hit_dict, 1, gt_cols, gt_path, 1000, base_outpath + name + "zero_noise/" + "sample" + str(idx) + "/result1/"):
                found["method1"] += 1
            if evaluate_view_search(es_time, values, viewSearch, columnInfer, candidate_columns, sample_score, hit_dict, 2, gt_cols, gt_path, 1000, base_outpath + name + "zero_noise/" + "sample" + str(idx) + "/result2/"):
                found["method2"] += 1
            if evaluate_view_search(es_time, values, viewSearch, columnInfer, candidate_columns, sample_score, hit_dict, 3, gt_cols, gt_path, 1000, base_outpath + name + "zero_noise/" + "sample" + str(idx) + "/result3/"):
                found["method3"] += 1
        f = open(base_outpath + name + "zero_noise" + "/hit.txt", "w")
        f.write(str(found))

        found["method1"] = 0
        found["method2"] = 0
        found["method3"] = 0
        for (idx, values) in enumerate(mid_noise_samples):
            if idx != 3:
                continue
            print("Processing mid noise samples no.", idx)
            attrs = ["", ""]
            es_start = time.time()
            candidate_columns, sample_score, hit_type_dict, match_dict, hit_dict = columnInfer.infer_candidate_columns(attrs, values, 600)
            es_time = time.time() - es_start
            if evaluate_view_search(es_time, values, viewSearch, columnInfer, candidate_columns, sample_score, hit_dict, 1, gt_cols, gt_path, 1000, base_outpath + name + "mid_noise/" + "sample" + str(idx) + "/result1/"):
                found["method1"] += 1
            if evaluate_view_search(es_time, values, viewSearch, columnInfer, candidate_columns, sample_score, hit_dict, 2, gt_cols, gt_path, 1000, base_outpath + name + "mid_noise/" + "sample" + str(idx) + "/result2/"):
                found["method2"] += 1
            if evaluate_view_search(es_time, values, viewSearch, columnInfer, candidate_columns, sample_score, hit_dict, 3, gt_cols, gt_path, 1000, base_outpath + name + "mid_noise/" + "sample" + str(idx) + "/result3/"):
                found["method3"] += 1
        f = open(base_outpath + name + "mid_noise" + "/hit.txt", "w")
        f.write(str(found))

        found["method1"] = 0
        found["method2"] = 0
        found["method3"] = 0
        for (idx, values) in enumerate(high_noise_samples):
            print("Processing high noise samples no.", idx)
            attrs = ["", ""]
            es_start = time.time()
            candidate_columns, sample_score, hit_type_dict, match_dict, hit_dict = columnInfer.infer_candidate_columns(attrs, values, 600)
            es_time = time.time() - es_start
            if evaluate_view_search(es_time, values, viewSearch, columnInfer, candidate_columns, sample_score, hit_dict, 1, gt_cols, gt_path, 1000, base_outpath + name + "high_noise/" + "sample" + str(idx) + "/result1/"):
                found["method1"] += 1
            if evaluate_view_search(es_time, values, viewSearch, columnInfer, candidate_columns, sample_score, hit_dict, 2, gt_cols, gt_path, 1000, base_outpath + name + "high_noise/" + "sample" + str(idx) + "/result2/"):
                found["method2"] += 1
            if evaluate_view_search(es_time, values, viewSearch, columnInfer, candidate_columns, sample_score, hit_dict, 3, gt_cols, gt_path, 1000, base_outpath + name + "high_noise/" + "sample" + str(idx) + "/result3/"):
                found["method3"] += 1
        f = open(base_outpath + name + "high_noise" + "/hit.txt", "w")
        f.write(str(found))