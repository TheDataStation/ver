from archived.DoD import column_infer
from archived.DoD import ViewSearchPruning
from knowledgerepr import fieldnetwork
from archived.modelstore import StoreHandler
from archived.DoD import FilterType
from archived.DoD import data_processing_utils as dpu


class DoD_Pipeline:
    def __init__(self, model_path, output_path, sep):
        self.model_path = model_path
        self.output_path = output_path
        self.sep = sep

        store_client = StoreHandler()
        network = fieldnetwork.deserialize_network(model_path)
        self.columnInfer = column_infer.ColumnInfer(network=network, store_client=store_client, csv_separator=sep)
        self.viewSearch = ViewSearchPruning(network=network, store_client=store_client, csv_separator=sep)

    def get_topk_views(self, attrs, values, k):
        candidate_columns, sample_score, hit_type_dict, match_dict, _ = self.columnInfer.infer_candidate_columns(attrs, values)
        results = self.columnInfer.view_spec_cluster_1(candidate_columns, sample_score)

        filter_drs = {}
        col_values = {}
        column_idx = 0
        for col, candidates in results.items():
            filter_drs[(col, FilterType.ATTR, column_idx)] = candidates
            col_values[col] = [row[column_idx] for row in values]
            column_idx += 1
        view_metadata_mapping = dict()
        i = 0
        perf_stats = dict()

        for mjp, attrs_project, metadata in self.viewSearch.search_views(col_values, filter_drs, perf_stats,
                                                                         max_hops=2,
                                                                         debug_enumerate_all_jps=False, offset=k):
            proj_view = dpu.project(mjp, attrs_project)
            full_view = False
            if self.output_path is not None:
                if full_view:
                    view_path = self.output_path + "/raw_view_" + str(i)
                    mjp.to_csv(view_path, encoding='latin1', index=False)
                view_path = self.output_path + "/view_" + str(i)
                proj_view.to_csv(view_path, encoding='latin1', index=False)  # always store this
                # store metadata associated to that view
                view_metadata_mapping[view_path] = metadata

            i += 1
        print("total views:", i)

