import json
import server_config as config
from DoD import column_infer
from DoD.utils import FilterType
from DoD.view_search_pruning import ViewSearchPruning
from knowledgerepr import fieldnetwork
from modelstore.elasticstore import StoreHandler
from DoD import data_processing_utils as dpu

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
    gt_cols = [(data["table1"], data["attr1"]), (data["table1"], data["attr1"])]
    return zero_noise_samples, mid_noise_samples, high_noise_samples, gt_cols

def found_gt_view_or_not(results, gt_cols):
    found = True
    for i, result in enumerate(results):
        if gt_cols[i] not in result:
            found = False
            break
    return found

def evaluate_view_search(vs, ci, candidate_columns, sample_score, hit_dict, flag, gt_cols, offset=1000, output_path=""):
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
    log = open(output_path + "/log.txt", "w")
    log.write("Method" + str(flag))
    for mjp, attrs_project, metadata, jp in vs.search_views({}, filter_drs, perf_stats, max_hops=2,
                                                            debug_enumerate_all_jps=False, offset=offset):
        log.write("view" + str(i))
        log.write(jp)
        log.write(attrs_project)
        if found:
            log.write("found")
        else:
            log.write("not found")
        proj_view = dpu.project(mjp, attrs_project)
        full_view = False

        if output_path is not None:
            if full_view:
                view_path = output_path + "/raw_view_" + str(i)
                mjp.to_csv(view_path, encoding='latin1', index=False)
            view_path = output_path + "/view_" + str(i) + ".csv"
            proj_view.to_csv(view_path, encoding='latin1', index=False)  # always store this

        i += 1
    print("total views:", i)
    return found

if __name__ == '__main__':
    zero_noise_samples, mid_noise_samples, high_noise_samples, gt_cols = loadData("tests/chembl_gt1.json")
    found = dict()
    found["method1"] = 0
    found["method2"] = 0
    found["method3"] = 0
    name = "chembl_gt1_"
    for (idx, values) in enumerate(zero_noise_samples):
        attrs = ["", ""]
        candidate_columns, sample_score, hit_type_dict, match_dict, hit_dict = columnInfer.infer_candidate_columns(attrs, values)
        if evaluate_view_search(viewSearch, columnInfer, attrs, values, 1, 1000, base_outpath + name + "zero_noise" + str(idx) + "/result1/"):
            found["method1"] += 1
        if evaluate_view_search(viewSearch, columnInfer, attrs, values, 2, 1000, base_outpath + name + "zero_noise" + str(idx) + "/result2/"):
            found["method2"] += 1
        if evaluate_view_search(viewSearch, columnInfer, attrs, values, 3, 1000, base_outpath + name + "zero_noise" + str(idx) + "/result3/"):
            found["method3"] += 1
    print(found)