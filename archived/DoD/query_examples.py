from archived.DoD import column_infer
from archived.DoD import FilterType
from archived.DoD import ViewSearchPruning
from knowledgerepr import fieldnetwork
from archived.modelstore import StoreHandler
from archived.DoD import data_processing_utils as dpu

model_path = '' # path to the network index
base_path = '' # path to the source tables
output_path = '' # path to output the views
sep = ',' # seperator used for csv files

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
    for mjp, attrs_project, metadata, jp, relations, jp_score in viewSearch.search_views({}, filter_drs, perf_stats, max_hops=1,
                                                            debug_enumerate_all_jps=False, offset=offset, dod_rank=False):
        
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

attrs = ["", ""]
values = [["HMA Egypt", "Louise Ellman", "UKBSC"], ["HMA Egypt", "Dr Sarah Wollaston", "UKBSC"]]

query_examples(attrs, values)