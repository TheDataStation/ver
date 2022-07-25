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

model_path = config.Chembl.path_model
sep = config.Chembl.separator
base_outpath = config.Chembl.output_path

store_client = StoreHandler()
network = fieldnetwork.deserialize_network(model_path)
columnInfer = column_infer.ColumnInfer(network=network, store_client=store_client, csv_separator=sep)
viewSearch = ViewSearchPruning(network=network, store_client=store_client, csv_separator=sep)

def loadData(path):
    data = json.load(open(path))
    zero_noise_samples = data["zero"]
    mid_noise_samples = data["mid"]
    high_noise_samples = data["high"]
    gt_cols = [(data["table1"], data["attr1"]), (data["table2"], data["attr2"])]
    gt_path = data["jp"]
    return zero_noise_samples, mid_noise_samples, high_noise_samples, gt_cols, gt_path

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

def evaluate_vs_time(vs, ci, candidate_columns, sample_score, hit_dict, flag, gt_cols, gt_path, offset=1000, output_path=""):
    filter_drs = {}
    perf_stats = dict()
    results = []
    start = time.time()
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
        results, _, results_hit = ci.view_spec_cluster2(candidate_columns, sample_score)
        idx = 0
        for column, _ in candidate_columns.items():
            filter_drs[(column, FilterType.ATTR, idx)] = results_hit[idx]
            idx += 1

        found = found_gt_view_or_not(results, gt_cols)
    for mjp, attrs_project, metadata, jp, relations in vs.search_views({}, filter_drs, perf_stats, max_hops=2,
                                                                       debug_enumerate_all_jps=False, offset=offset):

        pass
    duration = time.time() - start
    vs.clear_all_cache()
    return duration

def evaluate_view_search(vs, ci, candidate_columns, sample_score, hit_dict, flag, gt_cols, gt_path, offset=1000, output_path=""):
    filter_drs = {}
    perf_stats = dict()
    results = []

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
        results, _, results_hit = ci.view_spec_cluster2(candidate_columns, sample_score)
        idx = 0
        for column, _ in candidate_columns.items():
            filter_drs[(column, FilterType.ATTR, idx)] = results_hit[idx]
            idx += 1

        found = found_gt_view_or_not(results, gt_cols)
    i = 0
    if not os.path.exists(os.path.dirname(output_path)):
        try:
            os.makedirs(os.path.dirname(output_path))
        except OSError as exc:  # Guard against race condition
            if exc.errno != errno.EEXIST:
                raise
    log_path = output_path + "/log.txt"
    log = open(log_path, "w")
    log.write("Method" + str(flag) + "\n")
    if found:
        log.write("found ground truth\n")
    else:
        log.write("not found ground truth\n")
    gt_view = 0
    for mjp, attrs_project, metadata, jp, relations in vs.search_views({}, filter_drs, perf_stats, max_hops=2,
                                                            debug_enumerate_all_jps=False, offset=offset):
        log.write("view" + str(i) + "\n")
        log.write(jp + "\n")
        log.write(str(attrs_project) + "\n")
        log.write("#relations: " + str(relations) + "\n")
        print(jp)
        print(gt_path)
        if jp == gt_path:
            gt_view = i
        proj_view = dpu.project(mjp, attrs_project)
        full_view = False

        if output_path is not None:
            if full_view:
                view_path = output_path + "/raw_view_" + str(i)
                mjp.to_csv(view_path, encoding='latin1', index=False)
            view_path = output_path + "/view_" + str(i) + ".csv"
            proj_view.to_csv(view_path, encoding='latin1', index=False)  # always store this

        i += 1
    log.write("total views:" + str(i) + "\n")
    log.write("ground truth view: " +  "view" + str(gt_view) + ".csv" + "\n")
    log.write("#candidate group:" + str(perf_stats["num_candidate_groups"]) + "\n")
    log.write("#join_graphs:" + str(perf_stats["num_join_graphs"]) + "\n")
    return found

if __name__ == '__main__':
    zero_noise_samples, mid_noise_samples, high_noise_samples, gt_cols, gt_path = loadData("/home/cc/tests_chembl/chembl_gt5.json")
    f = open("/home/cc/experiments_chembl/time_5.txt", "a+")
    name = "chembl_gt5/"
    time_stat = dict()
    time_stat["method1"] = 0
    time_stat["method2"] = 0
    time_stat["method3"] = 0
    f.write(name + "\n")
    f.write("zero_noise\n")
    for (idx, values) in enumerate(zero_noise_samples):
        print("Processing zero noise samples no.", idx)
        attrs = ["", ""]
        candidate_columns, sample_score, hit_type_dict, match_dict, hit_dict = columnInfer.infer_candidate_columns(attrs, values)
        time1 = evaluate_vs_time(viewSearch, columnInfer, candidate_columns, sample_score, hit_dict, 1, gt_cols, gt_path, 1000, base_outpath + name + "zero_noise/" + "sample" + str(idx) + "/result1/")
        print("m1", time1)
        time_stat["method1"] += time1
        time2 = evaluate_vs_time(viewSearch, columnInfer, candidate_columns, sample_score, hit_dict, 2, gt_cols, gt_path, 1000, base_outpath + name + "zero_noise/" + "sample" + str(idx) + "/result2/")
        print("m2", time2)
        time_stat["method2"] += time2
        time3 = evaluate_vs_time(viewSearch, columnInfer, candidate_columns, sample_score, hit_dict, 3, gt_cols, gt_path, 1000, base_outpath + name + "zero_noise/" + "sample" + str(idx) + "/result3/")
        print("m3", time3)
        time_stat["method3"] += time3
    for k, v in time_stat.items():
        f.write(k + " " + str(round(v/5, 3)) + "\n")

    time_stat["method1"] = 0
    time_stat["method2"] = 0
    time_stat["method3"] = 0

    f.write("mid_noise\n")
    for (idx, values) in enumerate(mid_noise_samples):
        print("Processing mid noise samples no.", idx)
        attrs = ["", ""]
        candidate_columns, sample_score, hit_type_dict, match_dict, hit_dict = columnInfer.infer_candidate_columns(
            attrs, values)
        time1 = evaluate_vs_time(viewSearch, columnInfer, candidate_columns, sample_score, hit_dict, 1, gt_cols,
                                 gt_path, 1000, base_outpath + name + "zero_noise/" + "sample" + str(idx) + "/result1/")
        print("m1", time1)
        time_stat["method1"] += time1
        time2 = evaluate_vs_time(viewSearch, columnInfer, candidate_columns, sample_score, hit_dict, 2, gt_cols,
                                 gt_path, 1000, base_outpath + name + "zero_noise/" + "sample" + str(idx) + "/result2/")
        print("m2", time2)
        time_stat["method2"] += time2
        time3 = evaluate_vs_time(viewSearch, columnInfer, candidate_columns, sample_score, hit_dict, 3, gt_cols,
                                 gt_path, 1000, base_outpath + name + "zero_noise/" + "sample" + str(idx) + "/result3/")
        print("m3", time3)
        time_stat["method3"] += time3
    for k, v in time_stat.items():
        f.write(k + " " + str(round(v / 5, 3)) + "\n")

    time_stat["method1"] = 0
    time_stat["method2"] = 0
    time_stat["method3"] = 0
    f.write("high_noise\n")
    for (idx, values) in enumerate(high_noise_samples):
        print("Processing high noise samples no.", idx)
        attrs = ["", ""]
        candidate_columns, sample_score, hit_type_dict, match_dict, hit_dict = columnInfer.infer_candidate_columns(
            attrs, values)
        time1 = evaluate_vs_time(viewSearch, columnInfer, candidate_columns, sample_score, hit_dict, 1, gt_cols,
                                 gt_path, 1000, base_outpath + name + "zero_noise/" + "sample" + str(idx) + "/result1/")
        print("m1", time1)
        time_stat["method1"] += time1
        time2 = evaluate_vs_time(viewSearch, columnInfer, candidate_columns, sample_score, hit_dict, 2, gt_cols,
                                 gt_path, 1000, base_outpath + name + "zero_noise/" + "sample" + str(idx) + "/result2/")
        print("m2", time2)
        time_stat["method2"] += time2
        time3 = evaluate_vs_time(viewSearch, columnInfer, candidate_columns, sample_score, hit_dict, 3, gt_cols,
                                 gt_path, 1000, base_outpath + name + "zero_noise/" + "sample" + str(idx) + "/result3/")
        print("m3", time3)
        time_stat["method3"] += time3
    for k, v in time_stat.items():
        f.write(k + " " + str(round(v / 5, 3)) + "\n")

    # zero_noise_samples, mid_noise_samples, high_noise_samples, gt_cols, gt_path = loadData("/home/cc/tests3/chembl_gt1.json")
    # found = dict()
    # found["method1"] = 0
    # found["method2"] = 0
    # found["method3"] = 0
    # name = "chembl_gt1/"
    # for (idx, values) in enumerate(zero_noise_samples):
    #     print("Processing zero noise samples no.", idx)
    #     attrs = ["", ""]
    #     candidate_columns, sample_score, hit_type_dict, match_dict, hit_dict = columnInfer.infer_candidate_columns(attrs, values)
    #     if evaluate_view_search(viewSearch, columnInfer, candidate_columns, sample_score, hit_dict, 1, gt_cols, gt_path, 1000, base_outpath + name + "zero_noise/" + "sample" + str(idx) + "/result1/"):
    #         found["method1"] += 1
    #     if evaluate_view_search(viewSearch, columnInfer, candidate_columns, sample_score, hit_dict, 2, gt_cols, gt_path, 1000, base_outpath + name + "zero_noise/" + "sample" + str(idx) + "/result2/"):
    #         found["method2"] += 1
    #     if evaluate_view_search(viewSearch, columnInfer, candidate_columns, sample_score, hit_dict, 3, gt_cols, gt_path, 1000, base_outpath + name + "zero_noise/" + "sample" + str(idx) + "/result3/"):
    #         found["method3"] += 1
    # f = open(base_outpath + name + "zero_noise" + "/hit.txt", "w")
    # f.write(str(found))
    #
    # found["method1"] = 0
    # found["method2"] = 0
    # found["method3"] = 0
    # for (idx, values) in enumerate(mid_noise_samples):
    #     print("Processing mid noise samples no.", idx)
    #     attrs = ["", ""]
    #     candidate_columns, sample_score, hit_type_dict, match_dict, hit_dict = columnInfer.infer_candidate_columns(attrs, values)
    #     if evaluate_view_search(viewSearch, columnInfer, candidate_columns, sample_score, hit_dict, 1, gt_cols, gt_path, 1000, base_outpath + name + "mid_noise/" + "sample" + str(idx) + "/result1/"):
    #         found["method1"] += 1
    #     if evaluate_view_search(viewSearch, columnInfer, candidate_columns, sample_score, hit_dict, 2, gt_cols, gt_path, 1000, base_outpath + name + "mid_noise/" + "sample" + str(idx) + "/result2/"):
    #         found["method2"] += 1
    #     if evaluate_view_search(viewSearch, columnInfer, candidate_columns, sample_score, hit_dict, 3, gt_cols, gt_path, 1000, base_outpath + name + "mid_noise/" + "sample" + str(idx) + "/result3/"):
    #         found["method3"] += 1
    # f = open(base_outpath + name + "mid_noise" + "/hit.txt", "w")
    # f.write(str(found))
    #
    # found["method1"] = 0
    # found["method2"] = 0
    # found["method3"] = 0
    # for (idx, values) in enumerate(high_noise_samples):
    #     print("Processing high noise samples no.", idx)
    #     attrs = ["", ""]
    #     candidate_columns, sample_score, hit_type_dict, match_dict, hit_dict = columnInfer.infer_candidate_columns(attrs, values)
    #     if evaluate_view_search(viewSearch, columnInfer, candidate_columns, sample_score, hit_dict, 1, gt_cols, gt_path, 1000, base_outpath + name + "high_noise/" + "sample" + str(idx) + "/result1/"):
    #         found["method1"] += 1
    #     if evaluate_view_search(viewSearch, columnInfer, candidate_columns, sample_score, hit_dict, 2, gt_cols, gt_path, 1000, base_outpath + name + "high_noise/" + "sample" + str(idx) + "/result2/"):
    #         found["method2"] += 1
    #     if evaluate_view_search(viewSearch, columnInfer, candidate_columns, sample_score, hit_dict, 3, gt_cols, gt_path, 1000, base_outpath + name + "high_noise/" + "sample" + str(idx) + "/result3/"):
    #         found["method3"] += 1
    # f = open(base_outpath + name + "high_noise" + "/hit.txt", "w")
    # f.write(str(found))