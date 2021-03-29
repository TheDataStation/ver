from DoD import column_infer
from DoD.view_search_pruning import ViewSearchPruning
from knowledgerepr import fieldnetwork
from modelstore.elasticstore import StoreHandler
from DoD.utils import FilterType
from DoD import data_processing_utils as dpu

class DOD_API:

    def __init__(self, model_path, output_path, sep):
        self.model_path = model_path
        self.output_path = output_path
        self.sep = sep

        store_client = StoreHandler()
        network = fieldnetwork.deserialize_network(model_path)
        self.columnInfer = column_infer.ColumnInfer(network=network, store_client=store_client, csv_separator=sep)
        self.viewSearch = ViewSearchPruning(network=network, store_client=store_client, csv_separator=sep)

    """
    Get top k views without user interaction
    """
    def get_topk_views(self, attrs, values, k):
        candidate_columns, sample_score, hit_type_dict, match_dict = self.columnInfer.infer_candidate_columns(attrs, values)
        results = self.columnInfer.view_spec(candidate_columns, sample_score)

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

        for mjp, attrs_project, metadata in self.viewSearch.virtual_schema_iterative_search(col_values, filter_drs, perf_stats,
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

if __name__ == '__main__':
    path_model = "/Users/gongyue/aurum-datadiscovery/test/advwModels/"
    separator = ","
    output_path = "/Users/gongyue/aurum-datadiscovery/test/advwResult/"
    api = DOD_API(path_model, output_path, separator)

    attrs = ["", ""]
    # values = [["Amy", "Alberts", "European Sales Manager"],
    #           ["Ryan", "Cornelsen", "Production Technician - WC40"],
    #           ["Gary", "Altman", "Facilities Manager"]]
    values = [["Amy", "F"],
              ["Ryan", "M"],
              ["Gary", "M"],
              ["Ken", "M"],
              ["Terri", "F"]]
    api.get_topk_views(attrs, values, 5)
