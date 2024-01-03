import server_config as config
from archived.DoD import column_infer
from knowledgerepr import fieldnetwork
from archived.modelstore import StoreHandler
from archived.DoD import data_processing_utils as dpu

dataPath = "/home/cc/chemBL/"

# dataPath = "/home/cc/wdc_10000/"

class GroundTruth:
    table1 = ""
    attr1 = ""
    table2 = ""
    attr2 = ""
    join_path = ""

    def __init__(self, table1, attr1, table2, attr2, jp):
        self.table1 = table1
        self.attr1 = attr1
        self.zero_noise1 = ()
        self.noise1 = ()

        self.table2 = table2
        self.attr2 = attr2
        self.zero_noise2 = ()
        self.noise2 = ()

        self.join_path = jp
        model_path = config.Chembl.path_model
        sep = config.Chembl.separator
        self.base_outpath = config.Chembl.output_path

        # model_path = config.Wdc.path_model
        # sep = config.Wdc.separator
        # self.base_outpath = config.Wdc.output_path

        store_client = StoreHandler()
        network = fieldnetwork.deserialize_network(model_path)
        self.columnInfer = column_infer.ColumnInfer(network=network, store_client=store_client, csv_separator=sep)

    def set_noise_columns(self):
        drs1 = self.column_to_drs(self.table1, self.attr1)
        drs2 = self.column_to_drs(self.table2, self.attr2)
        similar_columns1 = self.columnInfer.aurum_api.content_similar_to(drs1)
        print(len(similar_columns1.data))
        similar_columns2 = self.columnInfer.aurum_api.content_similar_to(drs2)
        print(len(similar_columns2.data))
        res1, res1_zero = self.choose_noise_columns(self.table1, self.attr1, similar_columns1)
        if res1 is not None:
            self.noise1 = res1
            self.zero_noise1 = res1_zero
        else:
            print(self.attr1, "No valid noise")
            self.zero_noise1 = dpu.read_column(dataPath + self.table1, self.attr1)[self.attr1].dropna().drop_duplicates().values.tolist()
        res2, res2_zero = self.choose_noise_columns(self.table2, self.attr2, similar_columns2)
        if res2 is not None:
            self.noise2 = res2
            self.zero_noise2 = res2_zero
        else:
            print(self.attr2, "No valid noise")
            self.zero_noise2 = dpu.read_column(dataPath + self.table2, self.attr2)[self.attr2].dropna().drop_duplicates().values.tolist()

        # print(drs1)
        # print(drs2)
        # self.show_similar_columns(similar_columns1)
        # print("=============")
        # self.show_similar_columns(similar_columns2)

    def choose_noise_columns(self, table, attr, similar_columns):
        print("query_column", table, attr)
        # total_noise = []
        for x in similar_columns.data:
            df = dpu.read_column(dataPath + table, attr)
            try:
                df_noise = dpu.read_column(dataPath+x.source_name, x.field_name)
            except:
                continue
            # set1 = set(df[attr].dropna().drop_duplicates().values.tolist())
            # set2 = set(df_noise[x.field_name].dropna().drop_duplicates().values.tolist())
            # print("intersection:", len(set1.intersection(set2)))
            zero_noise = df[df[attr].isin(df_noise[x.field_name])][attr].dropna().drop_duplicates().values.tolist()
            print("len zero noise:", len(zero_noise))
            noise = df_noise[~df_noise[x.field_name].isin(df[attr])][x.field_name].dropna().drop_duplicates().values.tolist()
            print("len noise", len(noise))
            print("noise col:", x.source_name, x.field_name)
            # total_noise.extend(noise)
            if len(noise) >= 5 and len(zero_noise) >= 5:
                print("gt col:", table, attr)
                print("noise col:", x.source_name, x.field_name)
                return noise[0:50], zero_noise[0:50]
        return None, None

    def column_to_drs(self, table, attr):
        #res = self.columnInfer.aurum_api.search_exact_attribute(attr, max_results=100)
        res = self.columnInfer.aurum_api.search_table(table, max_results=50)
        print(len(res.data))
        drs = None
        for x in res.data:
            if x.source_name == table and x.field_name == attr:
                drs = x
        print(drs)
        return drs

    def show_similar_columns(self, similar_columns):
        for x in similar_columns.data:
            print(x.source_name, x.field_name)

class GroundTruth_extend:
    def __init__(self, attrs):
        self.attrs = attrs
        self.column_data = []
        model_path = config.Chembl.path_model
        sep = config.Chembl.separator
        self.base_outpath = config.Chembl.output_path

        store_client = StoreHandler()
        network = fieldnetwork.deserialize_network(model_path)
        self.columnInfer = column_infer.ColumnInfer(network=network, store_client=store_client, csv_separator=sep)

    def set_data(self):
        for attr in self.attrs:
            print(attr)
            data = dpu.read_column(dataPath + attr[0], attr[1])[attr[1]].dropna().drop_duplicates().values.tolist()
            self.column_data.append(data)
        print("column_data:", len(self.column_data))
    def get_data(self):
        return self.column_data
    
def getGroundTruths_Chembl():
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
    #
    # jp4 = "public.compound_records.csv-molregno JOIN public.molecule_dictionary.csv-molregno"
    # gt4 = GroundTruth("public.compound_records.csv", "compound_name", "public.molecule_dictionary.csv", "usan_stem", jp4)
    # gt4.set_noise_columns()
    # gt.append(gt4)
    #
    jp5 = "public.component_sequences.csv-component_id JOIN public.component_class.csv-component_id; public.component_class.csv-protein_class_id JOIN public.protein_classification.csv-protein_class_id"
    gt5 = GroundTruth("public.component_sequences.csv", "organism", "public.protein_classification.csv", "pref_name", jp5)
    gt5.set_noise_columns()
    gt.append(gt5)
    #
    # jp6 = "public.component_sequences.csv-component_id JOIN public.component_go.csv-component_id; public.component_go.csv-go_id JOIN public.go_classification.csv-go_id"
    # gt6 = GroundTruth("public.component_sequences.csv", "organism", "public.go_classification.csv", "pref_name", jp6)
    # gt6.set_noise_columns()
    # gt.append(gt6)

    jp7 = "public.component_sequences.csv-component_id JOIN public.component_class.csv-component_id; public.component_class.csv-protein_class_id JOIN public.protein_classification.csv-protein_class_id"
    gt7 = GroundTruth("public.component_sequences.csv", "organism", "public.protein_classification.csv", "short_name", jp7)
    gt7.set_noise_columns()
    gt.append(gt7)
    return gt

def getGroudTruths_Chembl_Extend():
    gts = []
    # two columns
    attr1 = [("public.assays.csv", "assay_organism"), ("public.cell_dictionary.csv", "cell_name")]
    gt1 = GroundTruth_extend(attr1)
    gt1.set_data()
    gts.append(gt1)

    # three columns
    attr2 = [("public.target_dictionary.csv", "organism"), ("public.assays.csv", "assay_organism"), ("public.cell_dictionary.csv", "cell_name")]
    gt2 = GroundTruth_extend(attr2)
    gt2.set_data()
    gts.append(gt2)

    # four columns
    attr3 = [("public.target_relations.csv", "relationship"), ("public.target_dictionary.csv", "organism"), ("public.assays.csv", "assay_organism"), ("public.cell_dictionary.csv", "cell_name")]
    gt3 = GroundTruth_extend(attr3)
    gt3.set_data()
    gts.append(gt3)

    return gts

def getGroundTruths_WDC():
    gt = []
    # jp1 = "1438042988061.16_20150728002308-00126-ip-10-236-191-2_880924977_1.json.csv-Title JOIN 1438042988061.16_20150728002308-00126-ip-10-236-191-2_880924977_3.json.csv-Nominated work"
    # gt1 = GroundTruth("1438042988061.16_20150728002308-00126-ip-10-236-191-2_880924977_1.json.csv", "Role", "1438042988061.16_20150728002308-00126-ip-10-236-191-2_880924977_3.json.csv", "Category", jp1)
    # gt1.set_noise_columns()
    # gt.append(gt1)
    # #
    # jp2 = "1438042988061.16_20150728002308-00126-ip-10-236-191-2_880924977_1.json.csv-Title JOIN 1438042988061.16_20150728002308-00126-ip-10-236-191-2_880924977_3.json.csv-Nominated work"
    # gt2 = GroundTruth("1438042988061.16_20150728002308-00126-ip-10-236-191-2_880924977_1.json.csv", "Title",
    #                   "1438042988061.16_20150728002308-00126-ip-10-236-191-2_880924977_3.json.csv", "Category", jp2)
    # gt2.set_noise_columns()
    # gt.append(gt2)
    #
    jp2 = "1438042988061.16_20150728002308-00307-ip-10-236-191-2_27730064_4.json.csv-Newspaper Title JOIN 1438042988061.16_20150728002308-00311-ip-10-236-191-2_31120164_1.json.csv-Title"
    gt2 = GroundTruth("1438042988061.16_20150728002308-00307-ip-10-236-191-2_27730064_4.json.csv", "State", "1438042988061.16_20150728002308-00311-ip-10-236-191-2_31120164_1.json.csv", "City", jp2)
    gt2.set_noise_columns()
    gt.append(gt2)
    #
    jp3 = "1438042988061.16_20150728002308-00202-ip-10-236-191-2_882777968_4.json.csv-state JOIN 1438042988061.16_20150728002308-00307-ip-10-236-191-2_27730064_4.json.csv-State"
    gt3 = GroundTruth("1438042988061.16_20150728002308-00202-ip-10-236-191-2_882777968_4.json.csv", "state",
                      "1438042988061.16_20150728002308-00307-ip-10-236-191-2_27730064_4.json.csv", "Newspaper Title",
                      jp3)
    gt3.set_noise_columns()
    gt.append(gt3)
    #
    # jp4 = "1438042988061.16_20150728002308-00202-ip-10-236-191-2_882777968_4.json.csv-state JOIN 1438042988061.16_20150728002308-00307-ip-10-236-191-2_27730064_4.json.csv-State"
    # gt4 = GroundTruth("1438042988061.16_20150728002308-00202-ip-10-236-191-2_882777968_4.json.csv", "city",
    #                   "1438042988061.16_20150728002308-00307-ip-10-236-191-2_27730064_4.json.csv", "Newspaper Title",
    #                   jp4)
    # gt4.set_noise_columns()
    # gt.append(gt4)

    # jp5 = "1438042988061.16_20150728002308-00004-ip-10-236-191-2_882198920_3.json.csv-city JOIN 1438042988061.16_20150728002308-00307-ip-10-236-191-2_27730064_4.json.csv-City"
    # gt5 = GroundTruth("1438042988061.16_20150728002308-00004-ip-10-236-191-2_882198920_3.json.csv", "venue",
    #                   "1438042988061.16_20150728002308-00307-ip-10-236-191-2_27730064_4.json.csv", "Newspaper Title",
    #                   jp5)
    # gt5.set_noise_columns()
    # gt.append(gt5)
    #
    # jp6 = "1438042988061.16_20150728002308-00126-ip-10-236-191-2_880924977_1.json.csv-Title JOIN 1438042988061.16_20150728002308-00126-ip-10-236-191-2_880924977_3.json.csv-Nominated work"
    # gt6 = GroundTruth("1438042988061.16_20150728002308-00126-ip-10-236-191-2_880924977_1.json.csv", "Title",
    #                   "1438042988061.16_20150728002308-00126-ip-10-236-191-2_880924977_3.json.csv", "Award", jp6)
    # gt6.set_noise_columns()

    # gt.append(gt6)
    return gt

if __name__ == '__main__':
    getGroudTruths_Chembl_Extend()