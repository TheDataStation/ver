from view_distillation import four_c
import view_presentation.ver as vp

from archived.DoD import column_infer, data_processing_utils as dpu
from archived.DoD import FilterType
from archived.DoD import ViewSearchPruning
from knowledgerepr import fieldnetwork
from archived.modelstore import StoreHandler

global model_path  # path to the network index
global base_path  # path to the source tables
global output_path  # path to output the views
global sep  # seperator used for csv files

store_client = StoreHandler()
network = fieldnetwork.deserialize_network(model_path)
columnInfer = column_infer.ColumnInfer(network=network, store_client=store_client, csv_separator=sep)
viewSearch = ViewSearchPruning(network=network, store_client=store_client, base_path=base_path, csv_separator=sep)


def query_examples(attrs, values):
    candidate_columns, sample_score, _, _, _ = columnInfer.infer_candidate_columns(attrs, values, 10000)
    print("getting candidate columns")
    _, _, results_hit = columnInfer.view_spec_cluster_union_find(candidate_columns, sample_score, output_path)
    idx = 0
    filter_drs = {}
    for column, _ in candidate_columns.items():
        filter_drs[(column, FilterType.ATTR, idx)] = results_hit[idx]
        idx += 1

    for k, hits in filter_drs.items():
        visited = set()
        pruned = []
        print("before", len(hits))
        for hit in hits:
            if hit.source_name in visited:
                continue
            pruned.append(hit)
            visited.add(hit.source_name)
        print("after", k, len(pruned))
        filter_drs[k] = pruned

    i = 0
    perf_stats = {}
    offset = 10000
    # begin search for views
    print("begin to materialize views")
    for mjp, attrs_project, metadata, jp, relations, jp_score in viewSearch.search_views({}, filter_drs, perf_stats,
                                                                                         max_hops=1,
                                                                                         debug_enumerate_all_jps=False,
                                                                                         offset=offset, dod_rank=False):

        proj_view = dpu.project(mjp, attrs_project).drop_duplicates()
        if relations == 1:
            proj_view = proj_view.head(2000).drop_duplicates()

        full_view = False
        if output_path is not None:
            if full_view:
                view_path = output_path + "/raw_view_" + str(i) + ".csv"
                mjp.to_csv(view_path, encoding='utf8', index=False)
            view_path = output_path + "/view_" + str(i) + ".csv"
            proj_view.to_csv(view_path, encoding='utf8', index=False)  # always store this
        i += 1
    print("finish materializing views")


def flatten(alist):
    return [x for l in alist for x in l]


if __name__ == '__main__':

    ########### Set parameters ###########

    model_path = ''  # path to the network index
    base_path = ''  # path to the source tables
    output_path = ''  # path to output the views
    sep = ','  # seperator used for csv files
    vd_results_dir = "" # result path for view distillation
    query_id = "0"

    ########### View Specification ###########

    attrs = ["", ""]
    values = [["example1", "example1"], ["example2", "example2"]]

    ########### Query by Examples ###########

    query_examples(attrs, values)

    ########### View Distillation ###########

    compatible_groups, removed_compatible_views, \
    largest_contained_views, removed_contained_views, contained_groups, \
    total_candidate_complementary_contradictory_views, \
    complementary_groups, \
    contradictory_groups, total_num_contradictory_pairs, \
    find_compatible_contained_time_total, \
    get_df_time, classify_per_table_schema_time, \
    total_identify_c1_time, total_identify_c2_time, total_num_comparisons_c2, \
    find_complementary_contradictory_time_total, total_find_candidate_keys_time, \
    schema_group, total_num_rows = \
    four_c.main(input_path=output_path)

    complementary_pairs = flatten(complementary_groups)
    contradictory_pairs = flatten(contradictory_groups)

    with open(f"{vd_results_dir}/candidate_compl_views_{query_id}.txt", "w") as f:
        for view in total_candidate_complementary_contradictory_views:
            f.write(view + "\n")

    with open(f"{vd_results_dir}/contained_groups_{query_id}.txt", "w") as f:
        for path1, group in contained_groups.items():
            f.write(f"{path1}: ")
            for path2 in group:
                f.write(f"{path2} ")
            f.write("\n")

    with open(f"{vd_results_dir}/compatible_groups_{query_id}.txt", "w") as f:
        for group in compatible_groups:
            for path in group:
                f.write(f"{path} ")
            f.write("\n")

    with open(f"{vd_results_dir}/compl_contra_pairs_{query_id}.txt", "w") as f:
        for path1, path2, candidate_key in complementary_pairs:
            f.write(f"{path1}, {path2}, {candidate_key}\n")
        for path1, path2, candidate_key, contradictory_key_value in contradictory_pairs:
            f.write(f"{path1}, {path2}, {candidate_key}, {contradictory_key_value}\n")

    ########### View Presentation ###########

    query_path = vd_results_dir

    with open(f"/{vd_results_dir}/{query_id}.csv", "w") as f:
        line_to_write = ""
        for attr in attrs:
            line_to_write += attr + ","
        f.write(line_to_write[:-1])
        for list in values:
            line_to_write = ""
            for value in list:
                line_to_write += value + ","
            f.write(line_to_write[:-1] + "\n")

    vp.interface(query_path, vd_results_dir, '', query_id)
