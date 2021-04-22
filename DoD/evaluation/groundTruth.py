import server_config as config
from DoD import column_infer
from knowledgerepr import fieldnetwork
from modelstore.elasticstore import StoreHandler
from DoD import data_processing_utils as dpu

dataPath = "/home/cc/chemBL/"

class GroundTruth:
    table1 = ""
    attr1 = ""
    table2 = ""
    attr2 = ""
    join_path = ""

    def __init__(self, table1, attr1, table2, attr2, jp):
        self.table1 = table1
        self.attr1 = attr1
        self.noise1 = ()

        self.table2 = table2
        self.attr2 = attr2
        self.noise2 = ()

        self.join_path = jp
        model_path = config.Chembl.path_model
        sep = config.Chembl.separator
        self.base_outpath = config.Chembl.output_path

        store_client = StoreHandler()
        network = fieldnetwork.deserialize_network(model_path)
        self.columnInfer = column_infer.ColumnInfer(network=network, store_client=store_client, csv_separator=sep)

    def set_noise_columns(self):
        drs1 = self.column_to_drs(self.table1, self.attr1)
        drs2 = self.column_to_drs(self.table2, self.attr2)
        similar_columns1 = self.columnInfer.aurum_api.content_similar_to(drs1)
        similar_columns2 = self.columnInfer.aurum_api.content_similar_to(drs2)
        res1 = self.choose_noise_columns(self.table1, self.attr1, similar_columns1)
        if res1 is not None:
            self.noise1 = res1
        else:
            print("No valid noise")
        res2 = self.choose_noise_columns(self.table2, self.attr2, similar_columns2)
        if res2 is not None:
            self.noise2 = res2
        else:
            print("No valid noise")
        # print(drs1)
        # print(drs2)
        # self.show_similar_columns(similar_columns1)
        # print("=============")
        # self.show_similar_columns(similar_columns2)

    def choose_noise_columns(self, table, attr, similar_columns):
        for x in similar_columns.data:
            df = dpu.read_column(dataPath + table, attr)
            df_noise = dpu.read_column(dataPath+x.source_name, x.field_name)
            noise = df_noise[~df_noise[x.field_name].isin(df[attr])][x.field_name].dropna().drop_duplicates().values.tolist()
            if len(noise) > 20:
                print("gt col:", table, attr)
                print("noise col:", x.source_name, x.field_name)
                return noise
        return None

    def column_to_drs(self, table, attr):
        res = self.columnInfer.aurum_api.search_exact_attribute(attr)
        drs = None
        for x in res.data:
            if x.source_name == table:
                drs = x
        return drs

    def show_similar_columns(self, similar_columns):
        for x in similar_columns.data:
            print(x.source_name, x.field_name)

def getGroundTruths():
    # gt-1
    gt = []
    jp1 = "public.assays.csv-assay_id JOIN public.activities.csv-assay_id; public.activities.csv-record_id JOIN public.compound_records.csv-record_id"
    gt1 = GroundTruth("public.assays.csv", "assay_organism", "public.compound_records.csv", "compound_name", jp1)
    gt1.set_noise_columns()
    gt.append(gt1)

    jp2 = "public.assays.csv-cell_id JOIN public.cell_dictionary.csv-cell_id"
    gt2 = GroundTruth("public.assays.csv", "assay_organism", "public.cell_dictionary.csv", "cell_name", jp2)
    gt2.set_noise_columns()
    gt.append(gt2)

    jp3 = "public.assays.csv-tid JOIN public.target_dictionary.csv-tid"
    gt3 = GroundTruth("public.assays.csv", "assay_organism", "public.target_dictionary.csv", "organism", jp3)
    gt3.set_noise_columns()
    gt.append(gt3)

    jp4 = "public.compound_records.csv-molregno JOIN public.molecule_dictionary.csv-molregno"
    gt4 = GroundTruth("public.compound_records.csv", "compound_name", "public.molecule_dictionary.csv", "usan_stem", jp4)
    gt4.set_noise_columns()
    gt.append(gt4)

    jp5 = "public.component_sequences.csv-component_id JOIN public.component_class.csv-component_id; public.component_class.csv-protein_class_id JOIN public.protein_classification.csv-protein_class_id"
    gt5 = GroundTruth("public.component_sequences.csv", "organism", "public.protein_classification.csv", "pref_name", jp5)
    gt5.set_noise_columns()
    gt.append(gt5)

    jp6 = "public.component_sequences.csv-component_id JOIN public.component_go.csv-component_id; public.component_go.csv-go_id JOIN public.go_classification.csv-go_id"
    gt6 = GroundTruth("public.component_sequences.csv", "organism", "public.go_classification.csv", "pref_name", jp6)
    gt6.set_noise_columns()
    gt.append(gt6)

    return gt
if __name__ == '__main__':
    getGroundTruths()