import server_config as config
from archived.DoD import column_infer
from archived.DoD import FilterType
from archived.DoD import ViewSearchPruning
from knowledgerepr import fieldnetwork
from archived.modelstore import StoreHandler
from chembl import loadData
import time

def count_join_stats(vs, ci, candidate_columns, sample_score, hit_dict, flag, offset=1000):
    filter_drs = {}
    perf_stats = dict()
    results = []
    dod_rank = False

    if flag == 1:
        idx = 0
        for column, candidates in candidate_columns.items():
            results.append([(c.source_name, c.field_name) for c in candidates])
            filter_drs[(column, FilterType.ATTR, idx)] = candidates
            idx += 1
    elif flag == 2:
        results = ci.view_spec_benchmark(sample_score)
        idx = 0
        for column, _ in candidate_columns.items():
            filter_drs[(column, FilterType.ATTR, idx)] = [hit_dict[x] for x in results[idx]]
            idx += 1
    elif flag == 3:
        dod_rank = True
        results, _, results_hit = ci.view_spec_cluster2(candidate_columns, sample_score)
        idx = 0
        for column, _ in candidate_columns.items():
            filter_drs[(column, FilterType.ATTR, idx)] = results_hit[idx]
            idx += 1

    start = time.time()
    num_candidate_groups, num_join_graphs = vs.count_joins({}, filter_drs, perf_stats, max_hops=2, debug_enumerate_all_jps=False, offset=offset,
                                 dod_rank=dod_rank, materialize=False)
    enumerate_time = time.time() - start
    return num_candidate_groups, num_join_graphs, enumerate_time

if __name__ == '__main__':
    sep = config.Chembl.separator
    base_outpath = config.Chembl.output_path
    base_path = config.Chembl.base_path
    base_model = "/home/cc/aurum-dod-staging/models/chemblModels_"
    f = open(base_outpath + "/diff_thresholds.txt", "w")
    for threshold in (0.8, 0.7, 0.6, 0.5):
        f.write("threshold-" + str(threshold) + "\n")
        model_path = base_model + str(threshold) + "/"
        store_client = StoreHandler()
        network = fieldnetwork.deserialize_network(model_path)
        columnInfer = column_infer.ColumnInfer(network=network, store_client=store_client, csv_separator=sep)
        viewSearch = ViewSearchPruning(network=network, store_client=store_client, base_path=base_path, csv_separator=sep)
        for num in range(5):
            zero_noise_samples, _, _, gt_cols, gt_path = loadData("/home/cc/tests_chembl_5_4/chembl_gt" + str(num) + ".json")
            name = "chembl_gt" + str(num) + "/"
            candidate_group_num_sum = 0
            join_graphs_num_sum = 0
            enum_time_sum = 0
            for (idx, values) in enumerate(zero_noise_samples):
                print("Processing zero noise samples no.", idx)
                attrs = ["", ""]
                es_start = time.time()
                candidate_columns, sample_score, hit_type_dict, match_dict, hit_dict = columnInfer.infer_candidate_columns(
                    attrs, values, 600)
                es_time = time.time() - es_start
                candidate_group_num, join_graphs_num, enum_time = count_join_stats(viewSearch, columnInfer, candidate_columns, sample_score, hit_dict, 1, 1000)

                candidate_group_num_sum += candidate_group_num
                join_graphs_num_sum += join_graphs_num
                enum_time_sum += enum_time

            f.write(str(round(candidate_group_num_sum/5, 3)) + " " + str(round(join_graphs_num_sum/5, 3)) + " " + str(round(enum_time_sum/5, 3)) + "\n")